

from functools import cached_property, cache, singledispatch as dispatch
import sqlite3, hashlib, re, os, logging, shutil, sys
from typing import Generator, Callable, Iterable, Self, overload, final, Literal, LiteralString, Any, TextIO, BinaryIO, Never, Iterator

from PseudoPathy import Path, DirectoryPath, FilePath, PathGroup, PathLibrary, PathList
from PseudoPathy.Library import CommonGroups
from PseudoPathy.PathShortHands import *

from MetaCanSNPerDatabases.Exceptions import *

class Mode: pass
class ReadMode: pass
class WriteMode: pass
class Direction: pass
class Nucleotides: pass

Mode        = Literal["r", "w"]
ReadMode    = Literal["r"]
WriteMode   = Literal["w"]
Direction   = Literal["DESC","ASC"]
Nucleotides = Nucleotides

formatPattern = re.compile(r"[{](.*?)[}]")

LOGGER = logging.Logger("MetaCanSNPerDatabases", level=logging.WARNING)

LOGGER_FILEHANDLER = logging.FileHandler("MetaCanSNPerDatabases.log")
LOGGER_FILEHANDLER.setFormatter(logging.Formatter("[%(name)s] %(asctime)s - %(levelname)s: %(message)s"))
LOGGER.addHandler(LOGGER_FILEHANDLER)

DATABASE_VERSIONS : dict[str,int] = {
    "7630f33662e27489b7bb7b3b121ca4ff" : 1, # Legacy CanSNPer
    "175c47f1ad61ec81a7d11d8a8e1887ff" : 2  # MetaCanSNPer Alpha version
}
LEGACY_VERSION = 0
CURRENT_VERSION = 2
CURRENT_TABLES_HASH = ""
CURRENT_INDEXES_HASH = ""
STRICT : bool = False
SOFTWARE_NAME = "MetaCanSNPer"

SOURCES = [
    "https://github.com/XJ-04561/MetaCanSNPer-data/raw/master/database/{databaseName}", # MetaCanSNPer
    "https://github.com/FOI-Bioinformatics/CanSNPer2-data/raw/master/database/{databaseName}" # Legacy CanSNPer
]

SOURCED = {"refseq":"F", "genbank": "A"}
NCBI_FTP_LINK = "ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GC{source}/{n1}/{n2}/{n3}/{genome_id}_{assembly}/{genome_id}_{assembly}_genomic.fna.gz"