#!/usr/local/bin/python
# vim: set tabstop=4 shiftwidth=4 autoindent smartindent:
'''MultiLock, manage lock files and parallel groups

>>> from multilock import MultiLock
>>> spamlock = MultiLock('spam')
>>> if spamlock.acquire():
... 	logging.debug('do some work')
... else:
... 	logging.debug('someone else is doing the work')
...
>>> 


You can also manage lock groups (default is a .locks) where multiple hosts can run parallel
jobs and wait for all locks in the group to clear before continuing 
e.g., 
>>> 
>>> from multilock import MultiLock
>>> spams = MultiLock(lockgroup='spams')
>>> spams.wait(3600)
>>> logging.debug('all spams are complete')
>>> 


You can also change the basepath (default is the current directory) and point to 
a shared mount if you have multiple hosts running different jobs in a lockgroup.
e.g.,
>>> from multilock import MultiLock
>>> spam = MultiLock('spam', 'spams', '/shared/path')
>>> if spam.acquire():
... 	logging.debug('do some work')
... else:
... 	logging.debug('someone else is doing the work')
... 
>>> 
'''
import time, socket, shutil, os, logging, errno


class MultiLockTimeoutException(Exception):
	pass

class MultiLockDeniedException(Exception):
	pass


class MultiLock:
	def __init__(self, lockname='lock', lockgroup='.locks', basepath='.', poll=0.5, nohup=False):
		''' MultiLock instance

			lockname: the name of this lock, default is 'lock'
			lockgroup: the name of the lockgroup, default is '.locks'
			basepath: the directory to store the locks, default is the current directory
			poll: the max time in seconds for a lock to be established, this must be larger
			      than the max time it takes to acquire a lock
		'''
		self.lockname = lockname
		self.basepath = os.path.realpath(basepath)
		self.lockgroup = os.path.join(self.basepath, lockgroup)
		self.lockfile = os.path.join(self.lockgroup, lockname, lockname + '.lock')
		self.lockedfile = os.path.join(self.lockgroup, lockname, lockname + '.locked')
		self.hostname = socket.gethostname()
		self.pid = os.getpid()
		self.poll = int(poll)
		self.fd = None
		self.nohup = nohup
		if nohup:
			self.pid = -1
		self._lockgroup()


	def _lockgroup(self):
		try:
			logging.debug('make sure that the lockgroup %s exists' %(self.lockgroup))
			os.makedirs(self.lockgroup)
		except OSError as exc:
			if exc.errno == errno.EEXIST:
				pass
			else:
				logging.error('fatal error trying to access lockgroup %s' %(self.lockgroup))
				raise


	def acquire(self, maxage=None):
		if not self.verify():
			logging.debug('you do not have the lock %s' %(self.lockedfile))
			if maxage:
				self.cleanup(maxage)
			self._lockgroup()
			try:
				logging.debug('attempt to create lock %s' %(self.lockfile))
				os.mkdir(os.path.dirname(self.lockfile))
				self.fd = os.open(self.lockfile, os.O_CREAT|os.O_EXCL|os.O_RDWR)
				os.write(self.fd, self.hostname+' '+str(self.pid))
				os.fsync(self.fd)
				os.close(self.fd)
			except OSError:
				logging.debug('unable to create lock %s' %(self.lockfile))
			else:
				try:
					logging.debug('attempt multilock %s' %(self.lockedfile))
					os.rename(self.lockfile, self.lockedfile)
					return self.verify()
				except OSError:
					logging.debug('unable to multilock %s' %(self.lockedfile))
		return 0


	def release(self):
		try:
			if self.verify():
				shutil.rmtree(os.path.dirname(self.lockedfile))
				try:
					logging.debug('released lock %s, will try to clean up lockgroup %s' %(self.lockname, self.lockgroup))
					os.rmdir(self.lockgroup)
				except OSError as exc:
					if exc.errno == errno.ENOTEMPTY:
						logging.debug('lockgroup %s is not empty' %(self.lockgroup))
						pass
					else:
						raise
		finally:
			return self.cleanup()


	def verify(self):
		logging.debug('test if this is your lock, %s' %(self.lockedfile))
		try:
			self.fd = os.open(self.lockedfile, os.O_RDWR)
			qhostname, qpid = os.read(self.fd, 1024).strip().split()
			os.close(self.fd)
			if qhostname != self.hostname or int(qpid) != int(self.pid):
				logging.debug('%s:%s claims to have the lock' %(qhostname, qpid))
				return 0
			logging.debug('success, you have lock %s' %(self.lockedfile))
			return 1
		except:
			logging.debug('you do not have lock %s' %(self.lockedfile))
			return 0


	def cleanup(self, maxage=None):
		''' safely cleanup any lock files or directories (artifacts from race conditions and exceptions)
		'''
		if maxage and os.path.exists(os.path.dirname(self.lockedfile)):
			try:
				tdiff = time.time() - os.stat(os.path.dirname(self.lockedfile))[8]
				if tdiff >= maxage:
					logging.debug('lock %s is older than maxage %s' %(os.path.dirname(self.lockedfile), maxage))
					shutil.rmtree(os.path.dirname(self.lockedfile))
			except:
				pass
		if os.path.isfile(self.lockedfile):
			logging.debug('potential cleanup, lock %s exists, checking hostname:pid' % (self.lockedfile))
			qhostname, qpid = (None, None)
			try:
				fh = open(self.lockedfile)
				qhostname, qpid = fh.read().strip().split()
				fh.close()
			except:
				pass
			if self.hostname == qhostname:
				try:
					if int(qpid) < 0:
						logging.debug('nohup lock %s, must manually release or timeout' %(self.lockedfile))
						return 1
					elif int(qpid) > 0:
						os.kill(int(qpid), 0)
				except OSError, e:
					if e.errno != errno.EPERM:
						logging.error('lock %s exists on this host, but pid %s is NOT running, force release' % (self.lockedfile, qpid))
						shutil.rmtree(os.path.dirname(self.lockedfile))
						return 1
					else:
						logging.debug('lock %s exists on this host but pid %s might still be running' %(self.lockedfile, qpid))
				else:
					logging.debug('lock %s exists on this host with pid %s still running' %(self.lockedfile, qpid))
			return 0
		return 1


	def wait(self, timeout=86400):
		logging.debug('waiting for lockgroup %s to complete' %(self.lockgroup))
		timeout = int(timeout)
		start_time = time.time()
		while True:
			try:
				if (time.time() - start_time) >= timeout:
					raise MultiLockTimeoutException("Timeout %s seconds" %(timeout))
				elif os.path.isdir(self.lockgroup):
					time.sleep(self.poll)
					os.rmdir(self.lockgroup)
				return 1
			except OSError as exc:
				if exc.errno == errno.ENOTEMPTY:
					pass
				elif exc.errno == errno.ENOENT:
					pass
				else:
					logging.error('fatal error waiting for %s' %(self.lockgroup))
					raise


	def __del__(self):
		if not self.nohup:
			self.release()

	
	def __enter__(self):
		''' pythonic 'with' statement

			e.g.,
			>>> with MultiLock('spam') as spam:
			... 	logging.debug('we have spam')
		'''
		if self.acquire():
			return self
		raise MultiLockDeniedException(self.lockname)


	def __exit__(self, type, value, traceback):
		''' executed after the with statement
		'''
		if self.verify():
			self.release()

