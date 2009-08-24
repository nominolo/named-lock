{-# OPTIONS_GHC -funbox-strict-fields #-}
-- |
-- Module      : Control.Concurrent.NamedLock
-- Copyright   : (c) Thomas Schilling 2009
-- License     : BSD-style
-- 
-- Maintainer  : nominolo@googlemail.com
-- Stability   : experimental
-- Portability : portable
-- 
-- This module implements \"named locks\".
-- 
-- A named lock is like a normal lock (@MVar ()@) but is created
-- on demand.  This is useful when you have a potentially infinite
-- number of resources that should not be used concurrently.
-- 
-- For example, in a web-server you might create a new lock for each
-- database query so that the same query is only run once.
-- 
-- Named locks are allocated in a 'LockPool'.  Names are arbitrary,
-- well-behaved instances of the 'Ord' class.
-- 
module Control.Concurrent.NamedLock
  ( -- * Creating Lock Pools
    newLockPool, LockPool,
    -- * Working with Named Locks
    grabNamedLock, releaseNamedLock, withNamedLock )
where

import Control.Concurrent
import qualified Data.Map as M
import Control.Exception ( block, unblock, onException )

newtype LockPool name = LockPool (MVar (M.Map name NLItem))
  
data NLItem = NLItem {-# UNPACK #-} !Int
                     {-# UNPACK #-} !(MVar ())

-- | Create a new, empty, lock pool.
newLockPool :: IO (LockPool name)
newLockPool = LockPool `fmap` newMVar M.empty

-- | Grab the lock with given name.  Blocks until the lock becomes
-- available.
grabNamedLock :: Ord name => LockPool name -> name -> IO ()
grabNamedLock (LockPool mvar) name = block $ do
  mp <- takeMVar mvar
  case M.lookup name mp of
    Nothing -> do
      -- No one currently holds the lock named 'name', so we create it.
      name_mvar <- newEmptyMVar
      let mp' = M.insert name (NLItem 1 name_mvar) mp
      putMVar mvar mp'
    Just (NLItem ctr name_mvar) -> do
      -- Someone is currently holding the lock.
      --
      -- 1. Increase the reference counter.
      let mp' = M.insert name (NLItem (ctr + 1) name_mvar) mp
          -- Integer overflow is possible in principle, but that would
          -- imply to have (maxBound :: Int) threads contending for
          -- the same lock, which seems very unlikely.
                
      -- 2. Release the outer lock.
      putMVar mvar mp'
              
      -- 3. Finally, wait for the lock to become available.
      takeMVar name_mvar

-- | Release the lock with the given name.
-- 
-- The released lock must have previously been grabbed via
-- 'grabNamedLock'.
releaseNamedLock :: Ord name => LockPool name -> name -> IO ()
releaseNamedLock (LockPool mvar) name = block $ do
  mp <- takeMVar mvar
  case M.lookup name mp of
    Nothing -> do
      putMVar mvar mp
      error $ "releaseNamedLock: cannot release non-existent lock." 

    Just (NLItem ctr name_mvar) -> do
      -- We must not delete the lock before every thread that was
      -- trying to get it has released it.  We use a reference counter
      -- to keep track of the number of threads that try to grab the
      -- lock.
      let mp' 
            | ctr > 1 = M.insert name (NLItem (ctr - 1) name_mvar) mp
            | otherwise = M.delete name mp
      putMVar mvar mp'
      -- Release the lock.  This will never block, since no two
      -- threads can write to the lock without having a reader
      -- waiting.
      putMVar name_mvar ()
      
-- | Hold the lock while running the action.
-- 
-- If the action throws an exception, the lock is released an the
-- exception propagated.  Returns the result of the action.
withNamedLock :: Ord name => LockPool name -> name -> IO a -> IO a
withNamedLock pool name action = block $ do
  grabNamedLock pool name
  unblock action `onException` releaseNamedLock pool name

{-
-- Use this for testing.
main = do
  lpool <- newLockPool
  sequence_ (replicate 20 (forkIO (worker lpool =<< myThreadId)))
  worker lpool =<< myThreadId
  where
    lock_names = ["a", "b", "c", "d", "e"]
    num_names = length lock_names
    worker lpool tid = do
      n <- (lock_names !!) `fmap` randomRIO (0, num_names - 1)
      putStrLn $ show tid ++ ": grabbing " ++ show n
      grabNamedLock lpool n
      --threadDelay 1000000
      putStrLn $ show tid ++ ": releasing " ++ show n
      releaseNamedLock lpool n
      worker lpool tid
-}
