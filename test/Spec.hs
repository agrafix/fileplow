module Main where

import Data.FilePlow

import Data.Monoid
import System.IO (hClose)
import System.IO.Temp
import Test.Hspec
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL

withDummyFile :: Char -> Int -> (FilePath -> IO a) -> IO a
withDummyFile c size go =
    withSystemTempFile "dummyFilePlowTest" $ \fp hdl ->
    do BS.hPut hdl (BSC.replicate size c)
       hClose hdl
       go fp

readRest :: MultiHandle Handle -> IO BS.ByteString
readRest hdl =
    do let getLoop builder =
               do eof <- pIsEOF hdl
                  if eof
                      then pure $ BSL.toStrict (BSB.toLazyByteString builder)
                      else do x <- pGetChar hdl
                              getLoop (builder <> BSB.char8 x)
       getLoop mempty

main :: IO ()
main =
    hspec $
    do describe "multi handle" $
           do it "simple get char works" $
                  withDummyFile 'a' 100 $ \f1 ->
                  withDummyFile 'b' 100 $ \f2 ->
                  withMultiHandle [f1, f2] $ \hdl ->
                  do bs <- readRest hdl
                     bs `shouldBe` (BSC.replicate 100 'a' <> BSC.replicate 100 'b')
              it "two full reads with going to the begining work" $
                  withDummyFile 'a' 100 $ \f1 ->
                  withDummyFile 'b' 100 $ \f2 ->
                  withMultiHandle [f1, f2] $ \hdl ->
                  do _ <- readRest hdl
                     pSeek hdl AbsoluteSeek 0
                     bs <- readRest hdl
                     bs `shouldBe` (BSC.replicate 100 'a' <> BSC.replicate 100 'b')
              it "seeked reads work" $
                  withDummyFile 'a' 100 $ \f1 ->
                  withDummyFile 'b' 100 $ \f2 ->
                  withMultiHandle [f1, f2] $ \hdl ->
                  do pSeek hdl AbsoluteSeek 100
                     bs <- readRest hdl
                     bs `shouldBe` BSC.replicate 100 'b'

                     pSeek hdl AbsoluteSeek 50
                     bs2 <- readRest hdl
                     bs2 `shouldBe` (BSC.replicate 50 'a' <> BSC.replicate 100 'b')

                     pSeek hdl SeekFromEnd (-50)
                     bs3 <- readRest hdl
                     bs3 `shouldBe` BSC.replicate 50 'b'

                     pSeek hdl SeekFromEnd (-150)
                     bs4 <- readRest hdl
                     bs4 `shouldBe` (BSC.replicate 50 'a' <> BSC.replicate 100 'b')

                     pSeek hdl AbsoluteSeek 0
                     pSeek hdl RelativeSeek 100
                     bs5 <- readRest hdl
                     bs5 `shouldBe` BSC.replicate 100 'b'

                     pSeek hdl AbsoluteSeek 50
                     pSeek hdl RelativeSeek 25
                     bs6 <- readRest hdl
                     bs6 `shouldBe` (BSC.replicate 25 'a' <> BSC.replicate 100 'b')
