{-# language
    DeriveGeneric
  , FlexibleContexts
  , FlexibleInstances
  , MultiParamTypeClasses
  , OverloadedStrings
  , StandaloneDeriving
  , TypeFamilies
#-}

-- | Defines a schema for the chinook example database
module Chinook.Schema where

import Database.Beam
import Database.Beam.Backend.SQL.BeamExtensions

import Data.Int
import Data.Text (Text)
import Data.Time (LocalTime)
import Data.Monoid
import Data.String
import Data.Scientific (Scientific)

import Database.Odpi.FromField (Exactly (..))

type Pk32 = Exactly Int32

-- * Address

data AddressMixin f
  = Address
  { address           :: Columnar f (Maybe Text)
  , addressCity       :: Columnar f (Maybe Text)
  , addressState      :: Columnar f (Maybe Text)
  , addressCountry    :: Columnar f (Maybe Text)
  , addressPostalCode :: Columnar f (Maybe Text)
  } deriving Generic
instance Beamable AddressMixin
type Address = AddressMixin Identity
deriving instance Show (AddressMixin Identity)

-- * Artist

data ArtistT f
  = Artist
  { artistId   :: Columnar f Pk32
  , artistName :: Columnar f Text
  } deriving Generic
instance Beamable ArtistT
type Artist = ArtistT Identity; deriving instance Show Artist
deriving instance Eq Artist

instance Table ArtistT where
  data PrimaryKey ArtistT f = ArtistId (Columnar f Pk32)
    deriving Generic
  primaryKey = ArtistId . artistId
instance Beamable (PrimaryKey ArtistT)
type ArtistId = PrimaryKey ArtistT Identity; deriving instance Show ArtistId
deriving instance Eq ArtistId

-- * Album

data AlbumT f
  = Album
  { albumId     :: Columnar f Pk32
  , albumTitle  :: Columnar f Text
  , albumArtist :: PrimaryKey ArtistT f
  } deriving Generic
instance Beamable AlbumT
type Album = AlbumT Identity; deriving instance Show Album
deriving instance Eq Album

instance Table AlbumT where
  data PrimaryKey AlbumT f = AlbumId (Columnar f Pk32)
    deriving Generic
  primaryKey = AlbumId . albumId
instance Beamable (PrimaryKey AlbumT)
type AlbumId = PrimaryKey AlbumT Identity; deriving instance Show AlbumId
deriving instance Show (PrimaryKey AlbumT (Nullable Identity))

artistAlbums :: OneToMany ChinookDb s ArtistT AlbumT
artistAlbums = oneToMany_ (album chinookDb) albumArtist

-- * Employee

data EmployeeT f
  = Employee
  { employeeId        :: Columnar f Pk32
  , employeeLastName  :: Columnar f Text
  , employeeFirstName :: Columnar f Text
  , employeeTitle     :: Columnar f (Maybe Text)
  , employeeReportsTo :: PrimaryKey EmployeeT (Nullable f)
  , employeeBirthDate :: Columnar f (Maybe LocalTime)
  , employeeHireDate  :: Columnar f (Maybe LocalTime)
  , employeeAddress   :: AddressMixin f
  , employeePhone     :: Columnar f (Maybe Text)
  , employeeFax       :: Columnar f (Maybe Text)
  , employeeEmail     :: Columnar f (Maybe Text)
  } deriving Generic
instance Beamable EmployeeT
type Employee = EmployeeT Identity; deriving instance Show Employee

instance Table EmployeeT where
  data PrimaryKey EmployeeT f = EmployeeId (Columnar f Pk32)
    deriving Generic
  primaryKey = EmployeeId . employeeId
instance Beamable (PrimaryKey EmployeeT)
type EmployeeId = PrimaryKey EmployeeT Identity; deriving instance Show EmployeeId
deriving instance Show (PrimaryKey EmployeeT (Nullable Identity))

-- * Customer

data CustomerT f
  = Customer
  { customerId        :: Columnar f Pk32
  , customerFirstName :: Columnar f Text
  , customerLastName  :: Columnar f Text
  , customerCompany   :: Columnar f (Maybe Text)
  , customerAddress   :: AddressMixin f
  , customerPhone     :: Columnar f (Maybe Text)
  , customerFax       :: Columnar f (Maybe Text)
  , customerEmail     :: Columnar f Text
  , customerSupportRep :: PrimaryKey EmployeeT (Nullable f)
  } deriving Generic
instance Beamable CustomerT
type Customer = CustomerT Identity; deriving instance Show Customer

instance Table CustomerT where
  data PrimaryKey CustomerT f = CustomerId (Columnar f Pk32)
    deriving Generic
  primaryKey = CustomerId . customerId
instance Beamable (PrimaryKey CustomerT)
type CustomerId = PrimaryKey CustomerT Identity
deriving instance Show CustomerId
deriving instance Eq CustomerId

data CustomerBroken1T f
  = CustomerBroken1
  { customerBroken1Id        :: Columnar f Pk32
  , customerBroken1Email     :: Columnar f (Maybe Text) -- this column is really non-nullable
  } deriving Generic
instance Beamable CustomerBroken1T
type CustomerBroken1 = CustomerBroken1T Identity; deriving instance Show CustomerBroken1

instance Table CustomerBroken1T where
  data PrimaryKey CustomerBroken1T f = CustomerBroken1Id (Columnar f Pk32)
    deriving Generic
  primaryKey = CustomerBroken1Id . customerBroken1Id
instance Beamable (PrimaryKey CustomerBroken1T)
type CustomerBroken1Id = PrimaryKey CustomerBroken1T Identity
deriving instance Show CustomerBroken1Id
deriving instance Eq CustomerBroken1Id

data CustomerBroken2T f
  = CustomerBroken2
  { customerBroken2Id        :: Columnar f Pk32
  , customerBroken2Company   :: Columnar f Text -- this column is really nullable
  } deriving Generic
instance Beamable CustomerBroken2T
type CustomerBroken2 = CustomerBroken2T Identity; deriving instance Show CustomerBroken2

instance Table CustomerBroken2T where
  data PrimaryKey CustomerBroken2T f = CustomerBroken2Id (Columnar f Pk32)
    deriving Generic
  primaryKey = CustomerBroken2Id . customerBroken2Id
instance Beamable (PrimaryKey CustomerBroken2T)
type CustomerBroken2Id = PrimaryKey CustomerBroken2T Identity
deriving instance Show CustomerBroken2Id
deriving instance Eq CustomerBroken2Id



-- * Genre

data GenreT f
  = Genre
  { genreId   :: Columnar f Pk32
  , genreName :: Columnar f Text
  } deriving Generic
instance Beamable GenreT
type Genre = GenreT Identity; deriving instance Show Genre

instance Table GenreT where
  data PrimaryKey GenreT f = GenreId (Columnar f Pk32)
    deriving Generic
  primaryKey = GenreId . genreId
instance Beamable (PrimaryKey GenreT)
type GenreId = PrimaryKey GenreT Identity; deriving instance Show GenreId
deriving instance Show (PrimaryKey GenreT (Nullable Identity))

-- * Invoice

data InvoiceT f
  = Invoice
  { invoiceId       :: Columnar f (SqlSerial Pk32) -- Slightly different from the standard chinook schema. Used for illustrative purposes in the docs
  , invoiceCustomer :: PrimaryKey CustomerT f
  , invoiceDate     :: Columnar f LocalTime
  , invoiceBillingAddress :: AddressMixin f
  , invoiceTotal    :: Columnar f Scientific
  } deriving Generic
instance Beamable InvoiceT
type Invoice = InvoiceT Identity; deriving instance Show Invoice

instance Table InvoiceT where
  data PrimaryKey InvoiceT f = InvoiceId (Columnar f (SqlSerial Pk32)) deriving Generic
  primaryKey = InvoiceId . invoiceId
instance Beamable (PrimaryKey InvoiceT)
type InvoiceId = PrimaryKey InvoiceT Identity; deriving instance Show InvoiceId

invoiceLines :: OneToMany ChinookDb s InvoiceT InvoiceLineT
invoiceLines = oneToMany_ (invoiceLine chinookDb) invoiceLineInvoice

-- * InvoiceLine

data InvoiceLineT f
  = InvoiceLine
  { invoiceLineId      :: Columnar f Pk32
  , invoiceLineInvoice :: PrimaryKey InvoiceT f
  , invoiceLineTrack   :: PrimaryKey TrackT f
  , invoiceLineUnitPrice :: Columnar f Scientific
  , invoiceLineQuantity :: Columnar f Pk32
  } deriving Generic
instance Beamable InvoiceLineT
type InvoiceLine = InvoiceLineT Identity; deriving instance Show InvoiceLine

instance Table InvoiceLineT where
  data PrimaryKey InvoiceLineT f = InvoiceLineId (Columnar f Pk32) deriving Generic
  primaryKey = InvoiceLineId . invoiceLineId
instance Beamable (PrimaryKey InvoiceLineT)
type InvoiceLineId = PrimaryKey InvoiceLineT Identity; deriving instance Show InvoiceLineId

-- * MediaType

data MediaTypeT f
  = MediaType
  { mediaTypeId   :: Columnar f Pk32
  , mediaTypeName :: Columnar f (Maybe Text)
  } deriving Generic
instance Beamable MediaTypeT
type MediaType = MediaTypeT Identity; deriving instance Show MediaType

instance Table MediaTypeT where
  data PrimaryKey MediaTypeT f = MediaTypeId (Columnar f Pk32) deriving Generic
  primaryKey = MediaTypeId . mediaTypeId
instance Beamable (PrimaryKey MediaTypeT)
type MediaTypeId = PrimaryKey MediaTypeT Identity; deriving instance Show MediaTypeId

-- * Playlist

data PlaylistT f
  = Playlist
  { playlistId :: Columnar f Pk32
  , playlistName :: Columnar f (Maybe Text)
  } deriving Generic
instance Beamable PlaylistT
type Playlist = PlaylistT Identity; deriving instance Show Playlist

instance Table PlaylistT where
  data PrimaryKey PlaylistT f = PlaylistId (Columnar f Pk32) deriving Generic
  primaryKey = PlaylistId . playlistId
instance Beamable (PrimaryKey PlaylistT)
type PlaylistId = PrimaryKey PlaylistT Identity; deriving instance Show PlaylistId

-- * PlaylistTrack

data PlaylistTrackT f
  = PlaylistTrack
  { playlistTrackPlaylistId :: PrimaryKey PlaylistT f
  , playlistTrackTrackId    :: PrimaryKey TrackT f
  } deriving Generic
instance Beamable PlaylistTrackT
type PlaylistTrack = PlaylistTrackT Identity; deriving instance Show PlaylistTrack

instance Table PlaylistTrackT where
  data PrimaryKey PlaylistTrackT f = PlaylistTrackId (PrimaryKey PlaylistT f) (PrimaryKey TrackT f)
    deriving Generic
  primaryKey = PlaylistTrackId <$> playlistTrackPlaylistId <*> playlistTrackTrackId
instance Beamable (PrimaryKey PlaylistTrackT)
type PlaylistTrackId = PrimaryKey PlaylistTrackT Identity; deriving instance Show PlaylistTrackId

playlistTrackRelationship :: ManyToMany ChinookDb PlaylistT TrackT
playlistTrackRelationship =
 manyToMany_ (playlistTrack chinookDb)
             playlistTrackPlaylistId
             playlistTrackTrackId

-- * Track

data TrackT f
  = Track
  { trackId           :: Columnar f Pk32
  , trackName         :: Columnar f Text
  , trackAlbumId      :: PrimaryKey AlbumT (Nullable f)
  , trackMediaTypeId  :: PrimaryKey MediaTypeT f
  , trackGenreId      :: PrimaryKey GenreT (Nullable f)
  , trackComposer     :: Columnar f (Maybe Text)
  , trackMilliseconds :: Columnar f Int32
  , trackBytes        :: Columnar f Int32
  , trackUnitPrice    :: Columnar f Scientific
  } deriving Generic
instance Beamable TrackT
type Track = TrackT Identity; deriving instance Show Track

instance Table TrackT where
  data PrimaryKey TrackT f = TrackId (Columnar f Pk32) deriving Generic
  primaryKey = TrackId . trackId
instance Beamable (PrimaryKey TrackT)
type TrackId = PrimaryKey TrackT Identity; deriving instance Show TrackId

genreTracks :: OneToManyOptional ChinookDb s GenreT TrackT
genreTracks = oneToManyOptional_ (track chinookDb) trackGenreId

mediaTypeTracks :: OneToMany ChinookDb s MediaTypeT TrackT
mediaTypeTracks = oneToMany_ (track chinookDb) trackMediaTypeId

albumTracks :: OneToManyOptional ChinookDb s AlbumT TrackT
albumTracks = oneToManyOptional_ (track chinookDb) trackAlbumId

-- * database

data ChinookDb entity
  = ChinookDb
  { album         :: entity (TableEntity AlbumT)
  , artist        :: entity (TableEntity ArtistT)
  , customer      :: entity (TableEntity CustomerT)
  , customerBroken1 :: entity (TableEntity CustomerBroken1T)
  , customerBroken2 :: entity (TableEntity CustomerBroken2T)
  , employee      :: entity (TableEntity EmployeeT)
  , genre         :: entity (TableEntity GenreT)
  , invoice       :: entity (TableEntity InvoiceT)
  , invoiceLine   :: entity (TableEntity InvoiceLineT)
  , mediaType     :: entity (TableEntity MediaTypeT)
  , playlist      :: entity (TableEntity PlaylistT)
  , playlistTrack :: entity (TableEntity PlaylistTrackT)
  , track         :: entity (TableEntity TrackT)
  } deriving Generic
instance Database be ChinookDb

addressFields b = Address (fromString (b <> "ADDRESS"))
                          (fromString (b <> "CITY"))
                          (fromString (b <> "STATE"))
                          (fromString (b <> "COUNTRY"))
                          (fromString (b <> "POSTALCODE"))

chinookDb :: DatabaseSettings be ChinookDb
chinookDb =
  defaultDbSettings `withDbModification`
  (dbModification
   { album = modifyTable (\_ -> "ALBUM")
                         (Album "ALBUMID" "TITLE" (ArtistId "ARTISTID"))
   , artist = modifyTable (\_ -> "ARTIST") (Artist "ARTISTID" "NAME")
   , customer = modifyTable (\_ -> "CUSTOMER")
                     (Customer "CUSTOMERID" "FIRSTNAME" "LASTNAME" "COMPANY"
                               (addressFields "") "PHONE" "FAX" "EMAIL"
                               (EmployeeId "SUPPORTREPID"))
   , customerBroken1 = modifyTable (\_ -> "CUSTOMER") (CustomerBroken1 "CUSTOMERID" "EMAIL")
   , customerBroken2 = modifyTable (\_ -> "CUSTOMER") (CustomerBroken2 "CUSTOMERID" "COMPANY")
   , employee = modifyTable (\_ -> "EMPLOYEE")
                    (Employee "EMPLOYEEID" "LASTNAME" "FIRSTNAME" "TITLE"
                              (EmployeeId "REPORTSTO") "BIRTHDATE" "HIREDATE"
                              (addressFields "") "PHONE" "FAX" "EMAIL")
   , genre = modifyTable (\_ -> "GENRE")
                  (Genre "GENREID" "NAME")
   , invoice = modifyTable (\_ -> "INVOICE")
                   (Invoice "INVOICEID" (CustomerId "CUSTOMERID") "INVOICEDATE"
                            (addressFields "BILLING") "TOTAL")
   , invoiceLine = modifyTable (\_ -> "INVOICELINE")
                        (InvoiceLine "INVOICELINEID" (InvoiceId "INVOICEID") (TrackId "TRACKID")
                                     "UNITPRICE" "QUANTITY")
   , mediaType = modifyTable (\_ -> "MEDIATYPE") (MediaType "MEDIATYPEID" "NAME")
   , playlist = modifyTable (\_ -> "PLAYLIST") (Playlist "PLAYLISTID" "NAME")
   , playlistTrack = modifyTable (\_ -> "PLAYLISTTRACK") (PlaylistTrack (PlaylistId "PLAYLISTID")
                                                                        (TrackId "TRACKID"))
   , track = modifyTable (\_ -> "TRACK") (Track "TRACKID" "NAME" (AlbumId "ALBUMID") (MediaTypeId "MEDIATYPEID")
                                                (GenreId "GENREID") "COMPOSER" "MILLISECONDS" "BYTES" "UNITPRICE")
   })
