

from functools import cached_property, cache
import sqlite3, hashlib, re, os, logging, shutil, sys
from typing import Generator, Callable, Iterable, Self, overload, final, Literal, LiteralString, Any, TextIO, BinaryIO, Never, Iterator

LOGGER = logging.Logger("MetaCanSNPerDatabases", level=logging.WARNING)

LOGGER_FILEHANDLER = logging.FileHandler("MetaCanSNPerDatabases.log")
LOGGER_FILEHANDLER.setFormatter(logging.Formatter("[%(name)s] %(asctime)s - %(levelname)s: %(message)s"))
LOGGER.addHandler(LOGGER_FILEHANDLER)
# type Mode = Literal["r", "w"]
# type ReadMode = Literal["r"]
# type WriteMode = Literal["w"]
# type Direction = Literal["DESC","ASC"]

DATABASE_VERSIONS : dict[str,int] = {
    "7630f33662e27489b7bb7b3b121ca4ff" : 1, # Legacy CanSNPer
    "175c47f1ad61ec81a7d11d8a8e1887ff" : 2  # MetaCanSNPer Alpha version
}
LEGACY_VERSION = 0
CURRENT_VERSION = 2
CURRENT_HASH = "175c47f1ad61ec81a7d11d8a8e1887ff"
STRICT : bool = False

SOURCES = [
    "https://github.com/XJ-04561/MetaCanSNPer-data/raw/master/database/{databaseName}", # MetaCanSNPer
    "https://github.com/FOI-Bioinformatics/CanSNPer2-data/raw/master/database/{databaseName}" # Legacy CanSNPer
]

SOURCED = {"refseq":"F", "genbank": "A"}
NCBI_FTP_LINK = "ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GC{source}/{n1}/{n2}/{n3}/{genome_id}_{assembly}/{genome_id}_{assembly}_genomic.fna.gz"