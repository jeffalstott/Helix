class Swarm(object):
    def __init__(self, memory_requirement=4, swarm_directory = '/home/alstottj/Code/swarmfiles/',\
            job_directory = '/home/alstottj/Code/jobfiles/', \
            python_location = '/usr/local/Python/2.7.2/bin/python'):
        self.memory_requirement =  memory_requirement
        self.swarm_directory = swarm_directory
        self.job_directory = job_directory
        self.python_location = python_location

        try:
            self.max_swarm = int(open(self.swarm_directory+'max_swarm_file.txt', 'r').read())
        except:
            print("Constructing max_swarm_file")
            from os import listdir
            swarms = [int(a) for a in listdir(swarm_directory)]
            if swarms:
                self.max_swarm = max(swarms)
            else:
                self.max_swarm = 0
        self.new_swarm = str(self.max_swarm+1)
        self.swarm_file_name = self.swarm_directory+self.new_swarm
        self.swarm_file = open(self.swarm_file_name, 'w')

    def add_job(self, job_string, no_python=False):
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
        self.swarm_file.close()
        from os import system
        print("Submitting analyses with swarm file "+self.swarm_file_name)
        system('swarm -f '+self.swarm_file_name+' -g '+str(self.memory_requirement)+' -m a')
        open(self.swarm_directory+'max_swarm_file.txt', 'w').write(self.new_swarm)
