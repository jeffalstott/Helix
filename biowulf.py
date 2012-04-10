__author__ = """\n""".join(['Jeff Alstott <jeffalstott@gmail.com>', 'Kenneth Daily <dailykm@mail.nih.gov'])

import subprocess
import tempfile

class Swarm(object):
    """A class for submitting jobs to Biowulf.

    __init__(self, memory_requirement=4, swarm_directory = '~/.swarmfiles/', job_directory = '~/.jobfiles/', python_location = '/usr/local/Python/2.7.2/bin/python')
    If they don't already exist, creates the directories swarm_directory and job_directory, where job and swarm scripts are written to.
    There is no cleanup for these scripts after the jobs are run and done, so one you're done with any debugging delete them manually.

    add_job(self, job_string, no_python=False)
    Adds the job job_string to the swarm file, prefaced by python_location. If you are not running a Python command or script, the job_string can be called directly, without the python_location, using the no_python=True keyword.

    submit(self)
    Submits the jobs added with add_job to the cluster using swarm, using the memory requirement given at initiation."""

    def __init__(self, memory_requirement=4, swarm_directory = '~/.swarmfiles/',\
            job_directory = '~/.jobfiles/', \
            python_location = '/usr/local/Python/2.7.2/bin/python'):
        self.memory_requirement =  memory_requirement
        from os.path import expanduser
        self.swarm_directory = expanduser(swarm_directory)
        self.job_directory = expanduser(job_directory)
        self.python_location = expanduser(python_location)

        import os, errno
        #Try to make the swarm and job file directories. If they already exist, move on
        try:
                os.makedirs(self.job_directory)
        except OSError, e:
                if e.errno != errno.EEXIST:
                            raise
        try:
                os.makedirs(self.swarm_directory)
        except OSError, e:
                if e.errno != errno.EEXIST:
                            raise

        try:
            self.max_swarm = int(open(self.swarm_directory+'max_swarm_file.txt', 'r').read())
        except:
            print("Constructing max_swarm_file")
            from os import listdir
            swarms = [int(a) for a in listdir(self.swarm_directory)]
            if swarms:
                self.max_swarm = max(swarms)
            else:
                self.max_swarm = 0
        self.new_swarm = str(self.max_swarm+1)
        self.swarm_file_name = self.swarm_directory+self.new_swarm
        self.swarm_file = open(self.swarm_file_name, 'w')

    def add_job(self, job_string, no_python=False):
        """Adds a job to be submitted with the swarm.
        
        If no_python=False, job_string is assumed to be Python code and written to a .py file in job_directory. A call to this .py file is then written in the swarm file.

        If no_python=True, job_string is a bash command, and put directly in the swarm file without creating a .py file in job_directory."""
        if no_python:
            self.swarm_file.write("%s\n" % (job_string))
            return

        try:
            self.max_job = int(open(self.job_directory+'max_job_file.txt', 'r').read())
        except:
            print("Constructing max_job_file")
            from os import listdir
            jobs_list = [int(a[:-3]) for a in listdir(self.job_directory)]
            if jobs_list:
                self.max_job = max(jobs_list)
            else:
                self.max_job = 0
        self.new_job = str(self.max_job+1)

        self.job_file_name = self.job_directory+self.new_job+'.py'
        self.job_file = open(self.job_file_name, 'w')
        ####Do the actual work!
        self.job_file.write(job_string)
        self.job_file.close()
        open(self.job_directory+'max_job_file.txt', 'w').write(str(self.new_job))

        self.swarm_file.write("%s %s\n" % (self.python_location, self.job_file_name)+\
                    ' 2>&1  > '+self.job_directory+self.new_job+'_out\n')

    def submit(self):
        """Submits all added jobs to Biowulf"""
        self.swarm_file.close()
        from os import system
        print("Submitting analyses with swarm file "+self.swarm_file_name)
        system('swarm -f '+self.swarm_file_name+' -g '+str(self.memory_requirement)+' -m a')
        open(self.swarm_directory+'max_swarm_file.txt', 'w').write(self.new_swarm)

class QSub(object):
    """A class for submitting jobs to Biowulf via qsub.
    
    A string of the script header and a string of the command to run are required.
    
    >>> qsub_object = QSub("echo Hello World")
    >>> qsub_stdout, qsub_stderr = qsub_object.submit(jobname="helloworld")

    """

    # Simplest script header for PBS job.
    _script_header = """#!/bin/bash
    """
    
    _qsub_command = "qsub -N %(jobname)s -l nodes=%(nodes)s %(params)s"
    
    def __init__(self, command):
        """Initialize the QSub object.
        
        The command that will be run is required.
        
        """
        
        self.command = command

    def _create_script_file(self, scriptfile_object=None):
        """Write out a script file.

        If an existing, script file object is not given, will create a temporary one.
        
        A given file object must be writable and WILL NOT be removed when closed.
        If the temporary file is used, it WILL be removed when closed.

        """

        if not scriptfile_object:
            scriptfile_object = tempfile.NamedTemporaryFile(dir="/scratch/")

        scriptfile_object.write("%(header)s\n%(command)s\n" % dict(header=self._script_header, command=self.command))
        scriptfile_object.file.flush()

        return scriptfile_object
        
    def submit(self, jobname, scriptfile_object=None, nodes=1, params="", stdout=None, stderr=None):
        """Run a command via qsub.
        
        Requires a header string and a job name.
        
        """

        assert jobname, "Job name is required!"
        
        scriptfile = self._create_script_file(scriptfile_object=scriptfile_object)
        
        qsub_cmd =  self._qsub_command % dict(jobname=jobname,
                                              nodes=nodes,
                                              params=params)

        
        # Redirect stderr and stdout
        if stdout:
            qsub_cmd += " -o %s" % stdout
        if stderr:
            qsub_cmd += " -e %s" % stderr

        # Set up the qsub command line call
        qsub_cmd = "%(cmd)s %(script)s" % dict(cmd=qsub_cmd, script=scriptfile.name)

        # run qsub call
        proc = subprocess.Popen(qsub_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        so, se = proc.communicate()

        # Clean up; script file is erased if a temporary one was used.
        # Maybe should catch keyboard interrupt to make sure the file is deleted then?
        scriptfile.close()

        return so, se

class QSubBlocking(QSub):
    """A class for submitting jobs to Biowulf via qsub in blocking mode.

    The qsub command waits for completion to exit; this is useful for pipelines that are
    controlled or managed outside of PBS.

    >>> qsub_object = QSubBlocking("echo Hello World")
    >>> qsub_stdout, qsub_stderr = qsub_object.submit(jobname="helloworld")
    
    """
    
    _qsub_command = "qsub -N %(jobname)s -l nodes=%(nodes)s -W block=true %(params)s"
