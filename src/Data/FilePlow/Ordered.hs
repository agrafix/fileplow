module Data.FilePlow.Ordered
    ( SeekTarget(..), seekTo )
where

import Data.FilePlow

import Numeric.Search
import qualified Data.ByteString as BS

data SeekTarget b
    = StLargerThan !b
    | StSmallerThan !b
    deriving (Show, Eq)

seekTo :: (Ord b, Eq b, PlowHandle hdl) => hdl -> SeekTarget b -> (BS.ByteString -> IO b) -> IO Bool
seekTo hdl st extract =
    do s <- pFileSize hdl
       ranges <- searchM (fromTo 0 s) divForever $ \pos ->
           do pSeek hdl AbsoluteSeek pos
              val <- pGetLine hdl >>= extract
              case st of
                StLargerThan x -> pure (val >= x)
                StSmallerThan x -> pure (val <= x)
       let tgtVal =
               case st of
                 StSmallerThan _ -> smallest True ranges
                 StLargerThan _ -> largest True ranges
       case tgtVal of
         Just pos -> pSeek hdl AbsoluteSeek pos >> pure True
         Nothing -> pure False
