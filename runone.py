#!/usr/local/bin/python
# vim: set tabstop=4 shiftwidth=4 autoindent smartindent:
'''
wrapper script to run one (and only one) instance of a command across multiple shared hosts

Alternatively, you can use this as a decorator to ensure that only one instance
of a decorated function is running (per lock per lockgroup)
e.g.,
>>>
>>> @runone
... def safe_process():
... 	pass
>>> 

You can specify lockname, lockgroup, and the basedir for the locking
e.g.,
>>> 
>>> @runone('billing', 'extract', '/shared/nfs')
... def extract('billing'):
...   # billing extract code
...
>>>

'''
import time, sys, subprocess, optparse, logging
from multilock import MultiLock


def runone(lockname='lock', lockgroup='.locks', basedir='.'):
	''' decorator with closure
		returns a function that will run one, and only one, instance per lockgroup
	'''
	def wrapper(fn):
		def new_fn(*args, **kwargs):
			return _runone(fn, lockname, lockgroup, basedir, *args, **kwargs)
		return new_fn
	return wrapper


def _runone(func, lockname, lockgroup, basedir, *args, **kwargs):
	''' run one, AND ONLY ONE, instance (respect locking)

		>>> 
		>>> _runone(print, 'lock', 'locks', '.', 'hello world')
		>>> 
	'''
	lock = MultiLock(lockname, lockgroup, basedir)
	if lock.acquire():
		func(*args, **kwargs)
		lock.release()


if __name__ == '__main__':

	p = optparse.OptionParser('usage: %prog [options] cmd [args]')
	p.add_option('--lockname', '-l', dest="lockname", default='lock', help="the lock name, should be unique for this instance")
	p.add_option('--lockgroup', '-g', dest="lockgroup", default='.locks', help="the lockgroup, a collection of locks independent locks")
	p.add_option('--basedir', '-d', dest="basedir", default='.', help="the base directory where the lock files should be written")
	p.add_option('--wait', '-w', dest="wait", default=None, help="optional, wait (up till the number of seconds specified) for all locks to complete in the lockgroup")
	options, args = p.parse_args()

	if options.wait:
		lock = MultiLock(options.lockname, options.lockgroup, options.basedir)
		lock.wait(options.wait)
		sys.exit()
		
	@runone(options.lockname, options.lockgroup, options.basedir)
	def _main():
		subprocess.call(args)

	_main()

