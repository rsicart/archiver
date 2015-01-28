#!/usr/bin/env python3

import subprocess
import sys, os, errno
import getopt
from datetime import date, datetime
import hashlib

try:
    import settings
except:
    print("No settings file created. Aborting.")
    sys.exit(1)


class Archiver:
    ''' Copies specified folders for a list of hosts to a target folder
        and cleans specified extension files from sources after.
    '''

    def __init__(self, arguments):
        self.setupArguments(arguments)
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
        print("./archive.py --run [--clean]")


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


    def buildCommands(self, namespace):
        ''' Replaces command templates and builds a list of commands
            ready to be executed.
            Returns a list of commands
        '''
        procs = []
        self.initProcessInfo(namespace)

        for source in self.sources:
            print("{} {} ...".format(namespace.capitalize(), source['host']))
            builtFolder = self.buildTargetFolder(source['host'], source['folder'])
            # fill command template with the right parameters
            if namespace == 'archive':
                command = 'rsync -avz {}@{}:{}/ {}'
                cmd = command.format(source['user'], source['host'], source['folder'], builtFolder)
            elif namespace == 'clean':
                command = 'ssh {}@{} find {} -type f -name \"*.{}\" -mtime +{} -exec rm {{}} \;'
                cmd = command.format(source['user'], source['host'], source['folder'], source['extension'], source['maxAge'])
            elif namespace == 'localchecksum':
                command = "find {} -type f -name *.{} -exec md5sum {{}} ;"
                cmd = command.format(builtFolder, source['extension'])
            elif namespace == 'remotechecksum':
                command = 'ssh {}@{} find {} -type f -name \"*.{}\" -exec md5sum {{}} \;'
                cmd = command.format(source['user'], source['host'], source['folder'], source['extension'])
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
        procs = self.buildCommands(namespace)
        self.runProcs(namespace, procs)


    def logProcessInfo(self, namespace):
        ''' Write stdout and stderr to log files
        '''
        # Format: 'date - host - line separator - output'
        logFormat = '{} - {} - {}\n'
        if settings.logging:
            for server, result in self.processInfo[namespace].items():
                if len(self.processInfo[namespace][server]['stdout']) > 0:
                    self.logFile.write(logFormat.format(datetime.now(), server, self.processInfo[namespace][server]['stdout']))
                if len(self.processInfo[namespace][server]['stderr']) > 0:
                    self.logFileError.write(logFormat.format(datetime.now(), server, self.processInfo[namespace][server]['stderr']))


    def verifyChecksums(self):
        ''' Read source and destination hash sums to determine
            if transfer was successfully finished.
            Important: checksum is calculated only for specified file extension.
        '''
        if self.errors:
            print("Won't execute *checksum* verification because of previous errors. Exiting.")
            sys.exit(6)

        # source folder (original files)
        name = namespace = remoteNamespace = 'remotechecksum'
        procs = self.buildCommands(namespace)
        self.runProcs(namespace, procs)

        for source in self.sources:
            localFolder = self.buildTargetFolder(source['host'], source['folder'])
            if not self.compareHashes(source, self.processInfo[remoteNamespace][source['host']]['stdout']):
                print("Checksum error on host {}".format(source['host']))
                sys.exit(5)


    def getHashes(self, stdout):
        ''' Returns a list of hashes extracted from a process output
            formated as 'md5sum_hash  filename'
        '''
        stdout = stdout.decode()
        a = stdout.split('\n')
        result = {}
        for line in a:
            list = line.split('  ')
            if len(list[0]) > 0:
                result.update({list[0]: list[1]})
        return result


    def calculateLocalHash(self, filename):
        ''' Calculates a hash from a local file and returns it
        '''
        hash = hashlib.md5()
        try:
            file = open(filename, 'rb')
            while True:
                data = file.read(128) # as recommended in doc
                if not data:
                    break
                hash.update(data)
        except:
            return False

        return hash.hexdigest()


    def compareHashes(self, source, stdoutRemote):
        ''' Compares remote file hash sums with same local files hash sums and returns a boolean
        '''
        if len(stdoutRemote) == 0:
            return False

        # local files are archived in self.targetFolder/host. See @self.buildTargetFolder
        localFolder = '{}/{}'.format(self.targetFolder, source['host'])
        hashesRemote = self.getHashes(stdoutRemote)
        for hash, filename in hashesRemote.items():
            # remote hash exists in local hashes and matches
            localHash = self.calculateLocalHash(localFolder + filename)
            if hash != localHash:
                print("Warning: remote hash {} different from local hash {} for file {}\n".format(hash, localHash, filename))
                return False
        return True


    def clean(self):
        ''' Clean all copied files from sources, one source at a time
        '''
        if not self.actions['clean']:
            sys.exit(0)

        if self.errors:
            print("Won't execute *clean* because of previous errors. Exiting.")
            sys.exit(7)

        name = namespace = self.clean.__name__
        procs = self.buildCommands(namespace)
        self.runProcs(namespace, procs)


    def run(self):
        archiver.archive()
        archiver.verifyChecksums()
        archiver.clean()


    def setupArguments(self, arguments):
        try:
            opts, args = getopt.getopt(arguments, "", ["help", "clean", "run"])
        except getopt.GetoptError:
            self.usage()
            sys.exit(2)

        self.actions = {
            'clean': False,
            'run': False,
        }

        ''' Parse script arguments
        '''
        for opt, arg in opts:
            if opt in ('-h', "--help"):
                self.usage()
                sys.exit(1)
            elif opt in ('--clean'):
                self.actions['clean'] = True
            elif opt in ('--run'):
                self.actions['run'] = True

        # --run protection
        if not self.actions['run']:
            self.usage()
            sys.exit(1)




if __name__ == '__main__':
    archiver = Archiver(sys.argv[1:])
    archiver.run()
