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
    , consolidate
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (async, race)
import Control.Concurrent.MVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.IO.Class
import Control.Monad.Loops
import Control.Monad.STM
import Data.Foldable as FD
import Data.Foldable as F
import Data.Function
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.IORef
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

data TSDirectedAcyclicGraph v a =
    TSDirectedAcyclicGraph
        { vertices :: !(TSH.TSHashTable v (v, Bool, a)) -- mapping of vertex to head-of-Sequence
        , topologicalSorted :: !(TSH.TSHashTable v (Seq v, a))
        , dependents :: !(TSH.TSHashTable v (MVar ())) -- 
        , baseVertex :: !(v)
        , lock :: !(MVar ())
        }

new :: (Eq v, Hashable v, Ord v, Show v, Num a) => v -> a -> Int16 -> Int16 -> IO (TSDirectedAcyclicGraph v a)
new def initval vertexParts topSortParts = do
    vertices <- TSH.new vertexParts
    dep <- TSH.new 1
    TSH.insert vertices def (def, True, 0)
    lock <- newMVar ()
    topSort <- TSH.new topSortParts
    TSH.insert topSort def (SQ.empty, initval)
    return $ TSDirectedAcyclicGraph vertices topSort dep def lock

getTopologicalSortedForest :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v a -> IO ([(v, Maybe v)])
getTopologicalSortedForest dag = do
    forest <- TSH.toList $ topologicalSorted dag
    return $
        L.concatMap
            (\(vt, (dg, _)) -> do
                 let rt =
                         if vt == baseVertex dag
                             then Nothing
                             else Just vt
                 L.map (\x -> do (x, rt)) (FD.toList dg))
            forest

getPrimaryTopologicalSorted :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v a -> IO ([v])
getPrimaryTopologicalSorted dag = do
    primary <- TSH.lookup (topologicalSorted dag) (baseVertex dag)
    case primary of
        Just (pdag, _) -> return $ FD.toList pdag
        Nothing -> return []

consolidate :: (Eq v, Hashable v, Ord v, Show v, Show a, Num a) => TSDirectedAcyclicGraph v a -> (a -> a -> a) -> IO ()
consolidate dag cumulate = do
    mindex <- TSH.lookupIndex (topologicalSorted dag) (baseVertex dag)
    case mindex of
        Just indx -> do
            indxRef <- newIORef (indx, baseVertex dag)
            continue <- newIORef True
            whileM_ (readIORef continue) $ do
                (ix, ky) <- readIORef indxRef
                res <- TSH.nextByIndex (topologicalSorted dag) (ky, ix + 1)
                print ("NEXT: ", res)
                case res of
                    Just (index, kn, (val, av)) -> do
                        writeIORef indxRef (index, kn)
                        tsd <- TSH.toList $ topologicalSorted dag
                        mapM (\(h, x) -> do print (h, F.toList x)) tsd
                        print ("===================================================")
                        fix
                            (\recur key -> do
                                 newh <- TSH.lookup (vertices dag) key
                                 case newh of
                                     Just (nh, _, na) -> do
                                         if nh == key
                                             then return ()
                                             else do
                                                 mx <- TSH.lookup (topologicalSorted dag) nh
                                                 case mx of
                                                     Just (m, am) -> do
                                                         TSH.insert
                                                             (topologicalSorted dag)
                                                             nh
                                                             ((m <> (kn <| val)), cumulate av am)
                                                         TSH.delete (topologicalSorted dag) kn
                                                     Nothing -> do
                                                         recur nh
                                     Nothing -> return ())
                            (kn)
                    Nothing -> writeIORef continue False -- end loop
    return ()

coalesce ::
       (Eq v, Hashable v, Ord v, Show v, Show a, Num a)
    => TSDirectedAcyclicGraph v a
    -> v
    -> [v]
    -> a
    -> (a -> a -> a)
    -> IO ()
coalesce dag vt edges aval cumulate = do
    takeMVar (lock dag)
    -- print ("vt", vt, "aval", aval)
    verts <- TSH.toList $ vertices dag
    -- print ("Vertices: ", verts)
    vals <-
        mapM
            (\dep -> do
                 fix -- do multi level recursive lookup
                     (\recur n -> do
                          res2 <- TSH.lookup (vertices dag) n
                          case res2 of
                              Just (ix, fl, v2) ->
                                  if n == ix
                                      then return ix
                                      else do
                                          y <- recur (ix)
                                          present <-
                                              TSH.mutateIO
                                                  (vertices dag)
                                                  n
                                                  (\ax ->
                                                       case ax of
                                                           Just (_, ff, aa) ->
                                                               if ff
                                                                   then return (Just (y, True, aa), True)
                                                                   else return (Just (y, True, aa), False)
                                                           Nothing -> return (Just (y, True, 100), False))
                                          frag <-
                                              TSH.mutateIO
                                                  (topologicalSorted dag)
                                                  n
                                                  (\fx ->
                                                       case fx of
                                                           Just f -> return (Nothing, Just f)
                                                           Nothing -> return (Nothing, Nothing))
                                          TSH.mutateIO
                                              (topologicalSorted dag)
                                              (y)
                                              (\mz ->
                                                   case mz of
                                                       Just (z, za) ->
                                                           case frag of
                                                               Just (fg, fa) -> do
                                                                   print ("parent", z, "frag:", fg)
                                                                   print (za, " ++ ", fa, "<=>", cumulate za fa)
                                                                   return (Just (z <> fg, cumulate za fa), ())
                                                               Nothing -> do
                                                                   if present
                                                                       then return (Just (z, za), ())
                                                                       else do
                                                                           print (za, " ++ ", v2, "=", cumulate za v2)
                                                                           return (Just (z |> n, cumulate za v2), ())
                                                       Nothing -> return (Just (SQ.singleton n, v2), ()))
                                          return y
                              Nothing -> do
                                  return n)
                     dep)
            edges
    putMVar (lock dag) ()
    if L.null vals
            -- takeMVar (lock dag)
        then do
            TSH.insert (vertices dag) vt (baseVertex dag, True, aval)
            TSH.mutateIO
                (topologicalSorted dag)
                (baseVertex dag)
                (\mz ->
                     case mz of
                         Just (z, za) -> return (Just (z |> vt, cumulate za aval), ())
                         Nothing -> return (Just (SQ.singleton vt, aval), ()))
        else do
            let head = vals !! 0
            if L.all (\x -> x == head) vals -- if all are same
                    -- takeMVar (lock dag)
                then do
                    seq <- TSH.lookup (vertices dag) (head)
                    case seq of
                        Just (_, _, vv) -> do
                            TSH.insert (vertices dag) vt (head, False, aval)
                            event <- TSH.lookup (dependents dag) vt
                            case event of
                                Just ev -> liftIO $ putMVar ev ()
                                Nothing -> return ()
                            -- putMVar (lock dag) ()
                        Nothing -> do
                            TSH.insert (vertices dag) vt (vt, False, aval)
                            -- putMVar (lock dag) ()
                            vrtx <- TSH.lookup (vertices dag) head
                            case vrtx of
                                Just (vx, fl, _) -> do
                                    return head -- vx
                                Nothing -> do
                                    event <-
                                        TSH.mutateIO
                                            (dependents dag)
                                            head
                                            (\x ->
                                                 case x of
                                                     Just e -> return (x, e)
                                                     Nothing -> do
                                                         em <- newEmptyMVar
                                                         return (Just em, em))
                                    ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (60 * 1000000))
                                    case ores of
                                        Right () -> throw InsertTimeoutException
                                        Left () -> do
                                            return head
                            coalesce dag vt [head] aval cumulate
                else do
                    TSH.insert (vertices dag) vt (vt, False, aval)
                    par <-
                        mapM
                            (\dep -> do
                                 vrtx <- TSH.lookup (vertices dag) dep
                                 case vrtx of
                                     Just (vx, fl, _) -> do
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
                                         ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (60 * 1000000))
                                         case ores of
                                             Right () -> throw InsertTimeoutException
                                             Left () -> do
                                                 return dep)
                            vals
                    let uniq = ST.toList $ ST.fromList par
                    coalesce dag vt uniq aval cumulate
    -- verts <- TSH.toList $ vertices dag
    -- print ("Vertices: ", verts)
--
--
--
--            
