{-# options_ghc -fno-warn-orphans #-}
{-# language
    FlexibleContexts
  , FlexibleInstances
  , GeneralizedNewtypeDeriving
  , MultiParamTypeClasses
  , OverloadedStrings
  , ScopedTypeVariables
  , TypeFamilies
  , TypeApplications
#-}
module Database.Beam.Oracle.Connection
  ( Oracle(..)
  , Ora(..)

  , runBeamOracle, runBeamOracleDebug
  , rowRep

  , OraCommandSyntax(..)
  , OraSelectSyntax(..), OraInsertSyntax(..)
  , OraUpdateSyntax(..), OraDeleteSyntax(..)
  , OraExpressionSyntax(..)
  ) where

import           Control.Applicative.Free
import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.ByteString.Builder
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import qualified Data.DList as DL
import           Data.Proxy (Proxy (..))
import           Data.Scientific (Scientific)
import qualified Data.Text as T
import           Data.Time (LocalTime)
import           Data.Word
import qualified Database.Odpi as Odpi
import           Database.Odpi.FromField

import           Database.Beam.Oracle.Syntax
import           Database.Beam.Backend.SQL


-- | The Oracle backend type, used to parametrize 'MonadBeam'. The
-- corresponding query monad is 'Ora'.
data Oracle = Oracle

instance BeamBackend Oracle where
  type BackendFromField Oracle = Odpi.FromField

instance Odpi.FromField SqlNull where
  fromField _ (Odpi.NativeNull _) = pure SqlNull
  fromField i v = Odpi.convError "SqlNull" i v

newtype Ora a = Ora { runOra :: ReaderT (String -> IO (), Odpi.Connection) IO a }
  deriving (Functor, Applicative, Monad, MonadIO)

data NotEnoughColumns
  = NotEnoughColumns Int Int
  deriving Show

instance Exception NotEnoughColumns where
  displayException (NotEnoughColumns colCnt needCnt) =
    unwords [ "Not enough columns while reading row. Only have"
            , show colCnt, "column(s) while", show needCnt, "would be needed." ]

runBeamOracleDebug :: (String -> IO ()) -> Odpi.Connection -> Ora a -> IO a
runBeamOracleDebug logger conn (Ora a) =
  runReaderT a (logger, conn)

runBeamOracle :: Odpi.Connection -> Ora a -> IO a
runBeamOracle = runBeamOracleDebug (\_ -> pure ())

rowRep :: FromBackendRowA Oracle a -> [Maybe Odpi.NativeTypeNum]
rowRep (Pure _) = []
rowRep (Ap (ParseOneField (_ :: (Maybe x -> f))) a) = (Odpi.nativeTypeFor (Proxy @x)) : rowRep a
rowRep (Ap (PeekField (_ :: (Maybe x -> f))) a) = rowRep a

-- | For each column optionally override the default return type.
--
-- Under the hood this uses dpiStmt_defineValue and is required if you're trying to
-- do non-standard things or to fetch full precision numbers as normally the return
-- type defaults to double.
defineValuesForRow :: forall a. FromBackendRow Oracle a
  => Proxy a -> Odpi.Statement -> Word32 -> IO ()
defineValuesForRow _ s ncol = do
  mapM_ f $ zip [1..ncol] (rowRep (fromBackendRow :: FromBackendRowA Oracle (Maybe a)))
  where
    f :: (Word32, Maybe Odpi.NativeTypeNum) -> IO ()
    f (i, mty) = maybe (pure ()) (Odpi.defineValueForTy s i) mty

instance MonadBeam OraCommandSyntax Oracle Odpi.Connection Ora where
  withDatabase = runBeamOracle
  withDatabaseDebug = runBeamOracleDebug

  runReturningMany (OraCommandSyntax _ (OraSyntax cmd vals)) (consume :: Ora (Maybe x) -> Ora a) = do
    (logger, conn) <- Ora ask
    let cmdStr = BL.toStrict $ toLazyByteString $ withPlaceholders cmd
    liftIO $ do
      logger (B8.unpack cmdStr ++ ";\n-- With values: " ++ show (DL.toList vals))
      Odpi.withStatement conn False cmdStr $ \st -> do
        bindValues st vals

        ncol <- Odpi.stmtExecute st Odpi.ModeExecDefault
        let needCols = valuesNeeded (fromBackendRow :: FromBackendRowA Oracle (Maybe x))
        unless (needCols <= fromIntegral ncol) $ throwIO $ NotEnoughColumns (fromIntegral ncol) needCols

        defineValuesForRow (Proxy @x) st ncol
        colInfo <- mapM (Odpi.stmtGetQueryInfo st) [1..ncol]
        runReaderT (runOra (consume $ nextRow st colInfo ncol)) (logger, conn)
    where
      bindValues :: Odpi.Statement -> DL.DList Odpi.NativeValue -> IO ()
      bindValues st vs = mapM_ (bindOne st) $ zip [1..] (DL.toList vs)

      bindOne st (pos, v) = Odpi.stmtBindValueByPos st pos v

-- | Action that fetches the next row
nextRow :: FromBackendRow Oracle x
  => Odpi.Statement -- ^ statement
  -> [Odpi.QueryInfo] -- ^ metadata about each column of the result
  -> Word32 -- ^ total number of columns
  -> Ora (Maybe x)
nextRow st colInfo ncol = Ora $ liftIO $ do
  mPageOffset <- Odpi.stmtFetch st
  case mPageOffset of
    Nothing -> pure Nothing
    Just _ -> do
      fields <- mapM (Odpi.stmtGetQueryValue st) [1..ncol]
      evalStateT (runAp step fromBackendRow) $ zip colInfo fields
  where
    step :: FromBackendRowF Oracle a -> StateT [(Odpi.QueryInfo, Odpi.NativeValue)] IO a
    step (ParseOneField next) = do
      (qi,df):fs <- get
      put fs
      case Odpi.isNativeNull df of
        True -> pure $ next Nothing
        False -> next . Just <$> liftIO (fromField qi df)
    step (PeekField _) = error "TODO: need PeekField after all"

instance FromBackendRow Oracle SqlNull
instance FromBackendRow Oracle Bool
instance FromBackendRow Oracle Word
instance FromBackendRow Oracle Word16
instance FromBackendRow Oracle Word32
instance FromBackendRow Oracle Word64
instance FromBackendRow Oracle Int
instance FromBackendRow Oracle Int16
instance FromBackendRow Oracle Int32
instance FromBackendRow Oracle Int64
instance FromBackendRow Oracle Float
instance FromBackendRow Oracle Double
instance FromBackendRow Oracle Scientific
instance FromBackendRow Oracle Char
instance FromBackendRow Oracle B.ByteString
instance FromBackendRow Oracle T.Text
instance FromBackendRow Oracle LocalTime

instance (FromField (Exactly a), FromBackendRow Oracle a) => FromBackendRow Oracle (Exactly a)
