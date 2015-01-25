#!/usr/bin/env python3

import subprocess
import sys, os, errno
from datetime import date, datetime

try:
	import settings
except:
	print("No settings file created. Aborting.")
	sys.exit(1)
	

class Archiver:
	''' Copies specified folders for a list of hosts to a target folder
		and cleans specified extension files from sources after.
	'''

	def __init__(self):
		self.targetFolder = settings.targetFolder
		self.sources = settings.sources
		self.verbose = False
		self.errors = False
		self.initProcessInfo()

		self.logFile = None
		self.logFileError = None
		if settings.logging:
			self.logFile = open(settings.logFile['stdout'], 'w')
			self.logFileError = open(settings.logFile['stderr'], 'w')


	def usage(self):
		print("usage")
	

	def initProcessInfo(self):
		self.processInfo = {}
		for source in self.sources:
			self.processInfo.update({
					source['host']: {
						'returncode': None,
						'stdout': '',
						'stderr': ''
					}
			})


	def buildTargetFolder(self, sourceHost, sourceFolder):
		''' Tries to execute mkdir -p to create /targetFolder/sourceHost/sourceFolder path
			and returns a string containing built folder
		''' 
		folder = '{}/{}{}'.format(self.targetFolder, sourceHost, sourceFolder)
		try:
			os.makedirs(folder)
		except OSError as exc: # Python >2.5
			if exc.errno == errno.EEXIST and os.path.isdir(folder):
				pass
			else: raise

		return folder


	def archive(self):
		''' Copies all files from sources to a target folder, one source at a time
		'''
		# command template
		command = 'rsync -avz {}@{}:{}/ {}'
		procs = []
		for source in self.sources:
			print("Archiving {}".format(source['host']))
			builtFolder = self.buildTargetFolder(source['host'], source['folder'])
			cmd = command.format(source['user'], source['host'], source['folder'], builtFolder)
			proc = subprocess.Popen(cmd.split(), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
			procs.append({'host': source['host'], 'proc': proc})
			print(cmd)

		while procs:
			for p in procs:
				server, proc = p['host'], p['proc']
				if proc.poll() is not None:
					self.processInfo[server]['stdout'] = proc.stdout.read()	
					self.processInfo[server]['stderr'] = proc.stderr.read()	
					self.processInfo[server]['returncode'] = proc.returncode
					if self.verbose:
						print(self.processInfo[server]['stdout'])
					if proc.returncode > 0 or len(self.processInfo[server]['stderr']) > 0:
						self.errors = True
						print(self.processInfo[server]['stderr'])
					# Delete finished process
					procs.remove(p)

		self.logProcessInfo()

		if self.errors:
			sys.exit(3)


	def logProcessInfo(self):
		# Format: 'date - host - line separator - output'
		logFormat = '{} - {}\n==============================\n{}'
		if settings.logging:
			for server, result in self.processInfo.items():
				if len(self.processInfo[server]['stdout']) > 0:
					self.logFile.write(logFormat.format(datetime.now(), server, self.processInfo[server]['stdout']))
				if len(self.processInfo[server]['stderr']) > 0:
					self.logFileError.write(logFormat.format(datetime.now(), server, self.processInfo[server]['stderr']))
		# Reset data
		self.initProcessInfo()
		sys.exit(3)
		

	def clean(self):
		''' Clean all copied files from sources, one source at a time
		'''
		# command template
		command = 'ssh {}@{} find {} -type f -name \"*.{}\" -mmin +{} -exec rm {{}} \;'
		#command = 'ssh {}@{} find {} -type f -name \"*.{}\" -mtime +{} -exec rm {{}} \;'
		procs = []
		for source in self.sources:
			print("Cleaning {}".format(source['host']))
			cmd = command.format(source['user'], source['host'], source['folder'], source['extension'], source['maxAge'])
			print(cmd)
			proc = subprocess.Popen(cmd.split(), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
			procs.append({'host': source['host'], 'proc': proc})

		while procs:
			for p in procs:
				server, proc = p['host'], p['proc']
				if proc.poll() is not None:
					self.processInfo[server]['stdout'] = proc.stdout.read()	
					self.processInfo[server]['stderr'] = proc.stderr.read()	
					self.processInfo[server]['returncode'] = proc.returncode
					if self.verbose:
						print(self.processInfo[server]['stdout'])
					if proc.returncode > 0 or len(self.processInfo[server]['stderr']) > 0:
						self.errors = True
						print(self.processInfo[server]['stderr'])
					# Delete finished process
					procs.remove(p)

		self.logProcessInfo()

		if self.errors:
			sys.exit(3)



if __name__ == '__main__':
	archiver = Archiver()
	archiver.archive()
	archiver.clean()
