{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph
    ( TSDirectedAcyclicGraph(..)
    , new
    , coalesce
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (async, race)
import Control.Concurrent.MVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.IO.Class
import Control.Monad.STM
import Data.Foldable as FD
import Data.Function
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.Int
import qualified Data.List as L
import Data.Sequence as SQ
import Data.Set as ST
import Data.Text as T
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Numeric as N

data DAGException =
    InsertTimeoutException
    deriving (Show)

instance Exception DAGException

data TSDirectedAcyclicGraph v =
    TSDirectedAcyclicGraph
        { vertices :: !(TSH.TSHashTable v (v, Bool)) -- mapping of vertex to head-of-Sequence
        , topologicalSorted :: !(TSH.TSHashTable v (Seq v))
        , dependents :: !(TSH.TSHashTable v (MVar ())) -- 
        , baseVertex :: !(v)
        , lock :: MVar ()
        }

new :: (Eq v, Hashable v, Ord v, Show v) => Int16 -> v -> IO (TSDirectedAcyclicGraph v)
new vsize def = do
    vertices <- TSH.new vsize
    dep <- TSH.new 1
    TSH.insert vertices def (def, True)
    lock <- newMVar ()
    topSort <- TSH.new 1
    TSH.insert topSort def (SQ.empty)
    return $ TSDirectedAcyclicGraph vertices topSort dep def lock

coalesce :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v -> v -> [v] -> IO ()
coalesce dag vt edges = do
    takeMVar (lock dag)
    vals <-
        mapM
            (\dep -> do
                 fix -- do multi level recursive lookup
                     (\recur n flg -> do
                          res2 <- TSH.lookup (vertices dag) n
                          case res2 of
                              Just (ix, fl) ->
                                  if n == ix
                                      then return ix
                                      else do
                                          y <- recur (ix) False
                                          -- print ("rett: ", y, " ix: ", ix, " n: ", n)
                                          TSH.insert (vertices dag) n (y, True)
                                          !frag <- TSH.lookup (topologicalSorted dag) n
                                          TSH.delete (topologicalSorted dag) n
                                          TSH.mutateIO
                                              (topologicalSorted dag)
                                              y
                                              (\mz ->
                                                   case mz of
                                                       Just z ->
                                                           case frag of
                                                               Just fg -> do
                                                                   return (Just $ z <> fg, ())
                                                               Nothing -> return (Just $ z |> n, ())
                                                       Nothing -> return (Just $ SQ.singleton n, ()))
                                          return y
                              Nothing -> do
                                  return n)
                     dep
                     True)
            edges
    putMVar (lock dag) ()
    if L.null vals
        then do
            takeMVar (lock dag)
            TSH.insert (vertices dag) vt (baseVertex dag, True)
            TSH.mutateIO
                (topologicalSorted dag)
                (baseVertex dag)
                (\mz ->
                     case mz of
                         Just z -> return (Just $ z |> vt, ())
                         Nothing -> return (Just $ SQ.singleton vt, ()))
            putMVar (lock dag) ()
        else do
            let head = vals !! 0
            if L.all (\x -> x == head) vals -- if all are same
                then do
                    takeMVar (lock dag)
                    seq <- TSH.lookup (vertices dag) (head)
                    case seq of
                        Just sq -> do
                            TSH.insert (vertices dag) vt (head, False)
                            event <- TSH.lookup (dependents dag) vt
                            case event of
                                Just ev -> liftIO $ putMVar ev () -- value in MVar, vt or head?
                                Nothing -> return ()
                            putMVar (lock dag) ()
                        Nothing -> do
                            TSH.insert (vertices dag) vt (vt, False)
                            putMVar (lock dag) ()
                            par <-
                                mapM
                                    (\dep -> do
                                         vrtx <- TSH.lookup (vertices dag) dep
                                         case vrtx of
                                             Just (vx, fl) -> do
                                                 return dep -- vx
                                             Nothing -> do
                                                 event <-
                                                     TSH.mutateIO
                                                         (dependents dag)
                                                         dep
                                                         (\x ->
                                                              case x of
                                                                  Just e -> return (x, e)
                                                                  Nothing -> do
                                                                      em <- newEmptyMVar
                                                                      return (Just em, em))
                                                 ores <-
                                                     LA.race
                                                         (liftIO $ readMVar event)
                                                         (liftIO $ threadDelay (10 * 1000000))
                                                 case ores of
                                                     Right () -> throw InsertTimeoutException
                                                     Left () -> do
                                                         return dep)
                                    vals
                            let uniq = ST.toList $ ST.fromList par
                            coalesce dag vt uniq
                else do
                    TSH.insert (vertices dag) vt (vt, False)
                    par <-
                        mapM
                            (\dep -> do
                                 vrtx <- TSH.lookup (vertices dag) dep
                                 case vrtx of
                                     Just (vx, fl) -> do
                                         return dep -- vx
                                     Nothing -> do
                                         event <-
                                             TSH.mutateIO
                                                 (dependents dag)
                                                 dep
                                                 (\x ->
                                                      case x of
                                                          Just e -> return (x, e)
                                                          Nothing -> do
                                                              em <- newEmptyMVar
                                                              return (Just em, em))
                                            --
                                         ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (10 * 1000000))
                                         case ores of
                                             Right () -> throw InsertTimeoutException
                                             Left () -> do
                                                 return dep)
                            vals
                    let uniq = ST.toList $ ST.fromList par
                    coalesce dag vt uniq
    -- verts <- TSH.toList $ vertices dag
    -- print ("Vertices: ", verts)
--
--
--
--            
