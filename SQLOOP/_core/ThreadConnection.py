
from threading import Thread, Lock
import sqlite3, logging
from queue import Queue, Empty as EmptyQueueException
import SQLOOP.Globals as Globals
import queue

class CursorLike:
	def __init__(self, data : list):
		self.data = data
		self.dataIterator = iter(data)
	def __iter__(self):
		for row in self.data:
			yield row
	def __next__(self):
		return next(self.dataIterator)
	def fetchone(self):
		if self.data:
			return self.data[0]
		else:
			return None
	def fetchall(self):
		return self.data

class ThreadConnection:

	LOG : logging.Logger = logging.getLogger(f"{Globals.SOFTWARE_NAME}.ThreadConnection")
	OPEN_DATABASES : dict[tuple[str, type],list["ThreadConnection",set[int]]]= {}
	REFERENCE : "ThreadConnection"
	CACHE_LOCK = Lock()
	CLOSED : bool

	queue : Queue[list[str,list,Lock, list]]
	queueLock : Lock
	running : bool
	
	filename : str
	_thread : Thread
	@property
	def _connection(self) -> "ThreadConnection":
		return self

	def __init__(self, filename : str, factory=sqlite3.Connection, identifier=0):
		with self.CACHE_LOCK:
			if (filename, factory) in self.OPEN_DATABASES and self.OPEN_DATABASES[filename, factory][0].running:
				ref = self.OPEN_DATABASES[filename, factory][0]
				self.OPEN_DATABASES[filename, factory][1].add(identifier)
				self.REFERENCE = ref
				if ref.running:
					return
				else:
					self.__dict__.pop("REFERENCE", None)
			
			self.OPEN_DATABASES[filename, factory] = [self, {identifier}]
			
			self.running = True
			self.CLOSED = False
			self.queue = Queue()
			self.queueLock = Lock()
			self.filename = filename
			self._factory = factory
			self._thread = Thread(target=self.mainLoop, daemon=True)
			self._thread.start()

	def __getattr__(self, name):
		return getattr(self.REFERENCE, name)

	def mainLoop(self):
		try:
			_connection = sqlite3.connect(self.filename, factory=self._factory)
			while self.running:
				try:
					string, params, lock, results = self.queue.get(timeout=15)
					if string is None and lock is None:
						continue
					try:
						results.extend(_connection.execute(string, params).fetchall())
					except Exception as e:
						self.LOG.exception(e)
						try:
							results.append(e)
						except:
							pass
					try:
						lock.release()
					except:
						pass
					self.queue.task_done()
				except EmptyQueueException:
					pass
				except Exception as e:
					self.LOG.exception(e)
			self.running = False
			_connection.close()
		except Exception as e:
			self.running = False
			_connection.close()
			self.LOG.exception(e)
			try:
				results.append(e)
				lock.release()
			except:
				pass
			for _ in range(self.queue.unfinished_tasks):
				string, params, lock, results = self.queue.get(timeout=15)
				if lock is not None:
					lock.release()

	def execute(self, string : str, params : list=[]):
		lock = Lock()
		lock.acquire()
		results = []
		with self.queueLock:
			self.queue.put([string, params, lock, results])
		
		if not self._thread.is_alive() or not self.running:
			raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
		
		lock.acquire()
		
		if results and isinstance(results[-1], Exception):
			raise results[-1]
		
		return CursorLike(results)
	
	def executemany(self, *statements : tuple[str, list]):
		fakeLock = lambda :None
		fakeLock.release = lambda :None

		with self.queueLock:
			results = [[] for _ in len(statements)]
			for i, statement in enumerate(statements[:-1]):
				self.queue.put([*statement, fakeLock, results[i]])
			lock = Lock()
			lock.acquire()
			self.queue.put([*statements[-1], lock, results[-1]])
		
		if not self._thread.is_alive() or not self.running:
			raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
		lock.acquire()
		
		if any(r and isinstance(r[-1], Exception) for r in results):
			raise next(filter(lambda r:r and isinstance(r[-1], Exception), results))[-1]
		
		return results

	def close(self, identifier=0):
		import inspect
		from pprint import pformat
		self.LOG.info(f"Database thread at 0x{id(self):X} was closed in stack:\n{pformat(inspect.stack())}")
		with self.CACHE_LOCK:
			self.OPEN_DATABASES[self.filename, self._factory][1].discard(identifier)
			if not self.OPEN_DATABASES[self.filename, self._factory][1]:
				if hasattr(self, "REFERENCE"):
					self.REFERENCE.running = False
				else:
					self.running = False
				self.queue.put([None, None, None, None])
				self._thread.join()
	
	def commit(self):
		self.execute("COMMIT;")

	def __del__(self):
		self.close()