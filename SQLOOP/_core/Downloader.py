
from SQLOOP.Globals import *
import SQLOOP.Globals as Globals

from threading import Thread

def watch(conn : sqlite3.Connection):
	while 

class ReportHook:
	def __init__(self, reportHook):
		self.reportHook = reportHook
		self.totalBlocks = None
	
	def __call__(self, block, blockSize, totalSize):
		if self.totalBlocks is None:
			self.totalBlocks = (totalSize // blockSize) + 1
		
		self.reportHook(block / self.totalBlocks)

class Downloader:

	directory : DirectoryPath
	SOURCES : tuple[tuple[str]] = ()
	reportHook : Callable = lambda block, blockSize, totalSize : None
	watcher : Thread
	
	"""Function that takes arguments: block, blockSize, totalSize"""
	_queueConnection : sqlite3.Connection
	_threads : list[Thread]= []

	def __init__(self, directory):
		if pAccess(directory, "rw"):
			self.directory = directory
		elif not pExists(directory) and pMakeDirs(directory, "rw"):
			self.directory = directory
		else:
			raise PermissionError(f"Missing read and/or write permissions in directory: {directory}")
		if fileNamePattern.fullmatch(SOFTWARE_NAME):
			self._queueConnection = sqlite3.connect(filename := f"{SOFTWARE_NAME}_{self.__class__.__name__}.db")
		else:
			self._queueConnection = sqlite3.connect(filename := f"{antiFileNamePattern.sub('-', SOFTWARE_NAME)}_{self.__class__.__name__}.db")
		self._queueConnection("CREATE TABLE IF NOT EXISTS queueTable (name TEXT UNIQUE, progress DECIMAL);")
		self.filename = filename

	def addSources(self, *sources : str):
		self.SOURCES = self.SOURCES + sources

	def isBusy(self, filename : str):
		return self._queueConnection.execute("SELECT CASE WHEN EXISTS(SELECT 1 FROM queueTable WHERE name = ?) THEN TRUE ELSE FALSE;", [filename]).fetchone()[0]
	
	def isDone(self, filename : str):
		return self._queueConnection.execute("SELECT CASE WHEN EXISTS(SELECT 1 FROM queueTable WHERE name = ? AND progress = 1.0) THEN TRUE ELSE FALSE;", [filename]).fetchone()[0]
	

	def updateProgress(self, name, prog : float):
		self._queueConnection.execute("UPDATE queueTable SET progress=? WHERE name = ?;", [prog, name])
		self.reportHook(prog)

	def getProgress(self, name):
		if (ret := self._queueConnection.execute("SELECT progress FROM queueTable WHERE name = ?;", [name]).fetchone()) is None:
			return None
		else:
			return ret[0]
	
	def runDownload(self, filename, reportHook=None):

		from urllib.request import urlretrieve
		reportHook = ReportHook(reportHook or self.reportHook)
		outFile = self.directory / filename
		tmpName = filename + ".tmp"
		for sourceName, sourceLink in self.SOURCES:
			try:
				(tmpName, msg) = urlretrieve(sourceLink.format(filename=filename), filename=outFile, reporthook=reportHook) # Throws error if 404
				os.rename(tmpName, outFile)
				reportHook.reportHook(1.0)
				return outFile, sourceName
			except Exception as e:
				LOGGER.info(f"Database {filename!r} not found/accessible on {sourceName!r}.")
				LOGGER.info(e)
		LOGGER.error(f"No database named {filename!r} found online. Sources tried: {', '.join(map(*this[0] + ": " + this[1], SOURCES))}")
		return None, None

	def updateLoop(self, filename : str, reportHook=None):
		
		reportHook = reportHook or self.reportHook
		d = 0.2
		while self.isBusy(filename):
			if (prog := self.getProgress(filename)) != 1.0:
				reportHook(prog)
				break
			reportHook(prog)
			sleep(d)
		else:
			self._queueConnection.execute("DELETE FROM queueTable WHERE name = ?;", [filename])
			try:
				self._queueConnection.execute("INSERT OR FAIL INTO queueTable VALUES (?, 0.0);", [filename])
				return 
			except:
				self.updateLoop(filename, reportHook=reportHook)
			
		
	def download(self, filename : str, reportHook=None, timeout=15) -> str|None:
		
		d = 0.2
		if self.isDone(filename):
			return filename, None
		elif self.isBusy(filename):
			mtime = 0
			while mtime != (mtime := os.stat(tmpName).st_mtime):
				if (prog := self.getProgress(filename)) == 1.0:
					break
				for i in range(timeout // d):
					self.reportHook(prog)
					sleep(d)
			else:
				self._queueConnection.execute("DELETE FROM queueTable WHERE name = ?;", [filename])
				return self.download(filename, reportHook=reportHook, timeout=timeout)
		elif 

		
