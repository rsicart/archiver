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
		self.processInfo = {}

		self.logFile = None
		self.logFileError = None
		if settings.logging:
			self.logFile = open(settings.logFile['stdout'], 'w')
			self.logFileError = open(settings.logFile['stderr'], 'w')


	def usage(self):
		print("usage")


	def initProcessInfo(self, namespace):
		for source in self.sources:
			self.processInfo.update({namespace:
				{
					source['host']: {
						'returncode': None,
						'stdout': '',
						'stderr': ''
					}
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


	def runProcs(self, namespace, processList):
		''' Checks a list of processes and removes the processes from the list when ready.
			Tries to get stdout, stderr and return code.
		'''
		while processList:
			for p in processList:
				server, proc = p['host'], p['proc']
				if proc.poll() is not None:
					self.processInfo[namespace][server]['stdout'] = proc.stdout.read()
					self.processInfo[namespace][server]['stderr'] = proc.stderr.read()
					self.processInfo[namespace][server]['returncode'] = proc.returncode
					if self.verbose:
						print(self.processInfo[namespace][server]['stdout'])
					if proc.returncode > 0 or len(self.processInfo[namespace][server]['stderr']) > 0:
						self.errors = True
						print(self.processInfo[namespace][server]['stderr'])
					# Delete finished process
					processList.remove(p)

		self.logProcessInfo(namespace)

		if self.errors:
			print('Method with namespace {} produced some errors. For detailed info read log files.'.format(namespace))
			sys.exit(3)


	def buildCommands(self, namespace, name):
		''' Replaces command templates and builds a list of commands
			ready to be executed.
			Returns a list of commands
		'''
		procs = []
		self.initProcessInfo(namespace)

		for source in self.sources:
			print("{} {}".format(name, source['host']))
			builtFolder = self.buildTargetFolder(source['host'], source['folder'])
			# fill command template with the right parameters
			if name == 'archive':
				command = 'rsync -avz {}@{}:{}/ {}'
				cmd = command.format(source['user'], source['host'], source['folder'], builtFolder)
			elif name == 'clean':
				command = 'ssh {}@{} find {} -type f -name \"*.{}\" -mtime +{} -exec rm {{}} \;'
				cmd = command.format(source['user'], source['host'], source['folder'], source['extension'], source['maxAge'])
			else:
				print("Invalid command")
				sys.exit(4)

			proc = subprocess.Popen(cmd.split(), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
			procs.append({'host': source['host'], 'proc': proc})
			print(cmd)

		return procs


	def archive(self):
		''' Copies all files from sources to a target folder, one source at a time
		'''
		name = namespace = self.archive.__name__
		procs = self.buildCommands(namespace, name)
		self.runProcs(namespace, procs)


	def logProcessInfo(self, namespace):
		''' Write stdout and stderr to log files
		'''
		# Format: 'date - host - line separator - output'
		logFormat = '{} - {} - {}'
		if settings.logging:
			for server, result in self.processInfo[namespace].items():
				if len(self.processInfo[namespace][server]['stdout']) > 0:
					self.logFile.write(logFormat.format(datetime.now(), server, self.processInfo[namespace][server]['stdout']))
				if len(self.processInfo[namespace][server]['stderr']) > 0:
					self.logFileError.write(logFormat.format(datetime.now(), server, self.processInfo[namespace][server]['stderr']))


	def verifyChecksums(self):
		''' Read source MD5SUMS and destination MD5SUMS to determine
			if transfer was successfully finished.
		'''
		# command template
		localCommand = "find {} -type f -name *.{} -exec md5sum {{}} ;"
		localNamespace = 'localchecksum'
		self.initProcessInfo(localNamespace)
		procs = []
		for source in self.sources:
			print("Verifying local {}".format(source['host']))
			builtFolder = self.buildTargetFolder(source['host'], source['folder'])
			cmd = localCommand.format(builtFolder, source['extension'])
			proc = subprocess.Popen(cmd.split(), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
			procs.append({'host': source['host'], 'proc': proc})

		while procs:
			for p in procs:
				server, proc = p['host'], p['proc']
				if proc.poll() is not None:
					self.processInfo[localNamespace][server]['stdout'] = proc.stdout.read()
					self.processInfo[localNamespace][server]['stderr'] = proc.stderr.read()
					self.processInfo[localNamespace][server]['returncode'] = proc.returncode
					if self.verbose:
						print(self.processInfo[localNamespace][server]['stdout'])
					if proc.returncode > 0 or len(self.processInfo[localNamespace][server]['stderr']) > 0:
						self.errors = True
						print(self.processInfo[localNamespace][server]['stderr'])
					# Delete finished process
					procs.remove(p)

		self.logProcessInfo(localNamespace)

		remoteCommand = 'ssh {}@{} find {} -type f -name \"*.{}\" -exec md5sum {{}} \;'
		remoteNamespace = 'remotechecksum'
		self.initProcessInfo(remoteNamespace)
		procs = []
		for source in self.sources:
			print("Verifying remote {}".format(source['host']))
			builtFolder = self.buildTargetFolder(source['host'], source['folder'])
			cmd = remoteCommand.format(source['user'], source['host'], source['folder'], source['extension'])
			print(cmd)
			proc = subprocess.Popen(cmd.split(), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
			procs.append({'host': source['host'], 'proc': proc})

		while procs:
			for p in procs:
				server, proc = p['host'], p['proc']
				if proc.poll() is not None:
					self.processInfo[remoteNamespace][server]['stdout'] = proc.stdout.read()
					self.processInfo[remoteNamespace][server]['stderr'] = proc.stderr.read()
					self.processInfo[remoteNamespace][server]['returncode'] = proc.returncode
					if self.verbose:
						print(self.processInfo[remoteNamespace][server]['stdout'])
					if proc.returncode > 0 or len(self.processInfo[remoteNamespace][server]['stderr']) > 0:
						self.errors = True
						print(self.processInfo[remoteNamespace][server]['stderr'])
					# Delete finished process
					procs.remove(p)

		self.logProcessInfo(remoteNamespace)

		for source in self.sources:
			#print(self.processInfo[localNamespace][server]['stdout'], self.processInfo[remoteNamespace][server]['stdout'])
			if not self.compareMd5sums(self.processInfo[localNamespace][server]['stdout'], self.processInfo[remoteNamespace][server]['stdout']):
				print("Checksum error on host {}".format(server))
				sys.exit(4)


	def getHashes(self, stdout):
		''' Returns a list of hashes extracted from a process output
			formated as 'md5sum_hash  filename'
		'''
		stdout = stdout.decode()
		a = stdout.split('\n')
		result = []
		for line in a:
			list = line.split('  ')
			result.append(list[0])
		return result


	def compareMd5sums(self, stdoutA, stdoutB):
		''' Compares two process outputs and returns a boolean
		'''
		hashesA = self.getHashes(stdoutA)
		hashesB = self.getHashes(stdoutB)
		return hashesA == hashesB


	def clean(self):
		''' Clean all copied files from sources, one source at a time
		'''
		raise NotImplementedError("WIP. Must implement checksum checking before.")

		name = namespace = self.clean.__name__
		procs = self.buildCommands(namespace, name)
		self.runProcs(namespace, procs)


	def run(self):
		archiver.archive()
		archiver.clean()



if __name__ == '__main__':
	archiver = Archiver()
	archiver.run()
