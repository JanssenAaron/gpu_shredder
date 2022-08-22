import re
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy.orm import declarative_base

Base = declarative_base()
# Used to get the sql alchemy metadata of the classes in this file
def getBase():
    return Base

# This is the class that sql alchemy uses to contruct the table and its objects represent records in that table

# This objects of this class should be gotten from sql_job.export_to_alchemy() method

class alchemy_class_sql_job(Base):
    __tablename__ = "jobs"

    job_id = Column(String(100), primary_key=True)
    ngpus = Column(Integer)         # gpus
    energy_used = Column(Float)     # watts
    max_mem = Column(Float)         # GB
    gpu_duration = Column(Float)    # hrs
    starttime = Column(Integer)     # Timestamp
    endtime = Column(Integer)       # Timestamp
    used_walltime = Column(Integer) # Secs

# Used to store info about jobs that is shared across nodes
class sql_job:

    def __init__(self, id, job_obj):
        self.job_id = id
        self.ngpus = job_obj.get_resource_list("ngpus")
        self.endtime = job_obj.get_data("end")
        self.starttime = job_obj.get_data("start")

    def export_to_alchemy(self):
        return alchemy_class_sql_job(
                job_id = self.job_id,
                ngpus = self.ngpus,
                starttime = self.starttime,
                endtime = self.endtime
        )


# This is the class that sql alchemy uses to construct the table and its objects represent records in that table

# This objects of this class should be gotten from gpu_usage.export_to_alchemy() method

class alchemy_class_gpu_usage(Base):
    __tablename__ = "gpu_metrics"

    job_id = Column(String(100), primary_key=True)
    node_name_and_gpu_number = Column(String(100), primary_key=True)
    mem_clock_avg = Column(Integer)           # MHz
    mem_util_max = Column(Float)              # Percent %
    mem_used_max = Column(Float)              # GB
    mem_util_avg = Column(Float)              # Percent %
    sm_clock_avg = Column(Integer)            # MHz
    sm_util_avg = Column(Float)               # Percent %
    energy_used = Column(Float)               # Watt
    gpu_duration = Column(Float)              # hrs

# This class is used to store a node's gpu's metrics for one job
# This is used for pre database code
class gpu_usage:
    # List of keys that MUST be in the dictonary passed into the init method
    valid_keys = [
        "GPU_memoryClock_average_per_node_gpu",
        "GPU_memoryUtilization_maxValue_per_node_gpu",
        "GPU_smClock_average_per_node_gpu",
        "GPU_smUtilization_average_per_node_gpu",
        "GPU_memoryUtilization_average_per_node_gpu",
        "GPU_energyConsumed_per_node_gpu",
        "GPU_maxGpuMemoryUsed_per_node_gpu",
        "GPU_duration_per_node_gpu"
    ]
    def __init__(self, id, dict):
        self.node_name_and_gpu_number = id
        self.load_dict(dict)
        
    # Returns a sql alchemly compatiable object 
    def export_to_alchemy(self):
        return alchemy_class_gpu_usage(
            job_id = self.job_id,
            node_name_and_gpu_number = self.node_name_and_gpu_number,
            mem_clock_avg = self.mem_clock_avg,
            mem_util_max = self.mem_util_max,
            mem_used_max = self.mem_used_max,
            mem_util_avg = self.mem_util_avg,
            sm_clock_avg = self.sm_clock_avg,
            sm_util_avg = self.sm_util_avg,
            energy_used = self.energy_used,
            gpu_duration = self.gpu_duration
        )
    # Set members from a dictonary
    def load_dict(self, dict):
        if len(dict) != len(gpu_usage.valid_keys) + 1: # 1 for id
            raise Exception("The number of expected metrics does not match the actual")
        self.job_id         =  dict["jobid"]
        self.mem_clock_avg  =  dict["GPU_memoryClock_average_per_node_gpu"]
        self.mem_util_max   =  dict["GPU_memoryUtilization_maxValue_per_node_gpu"]
        self.mem_used_max   =  dict["GPU_maxGpuMemoryUsed_per_node_gpu"]
        self.mem_util_avg   =  dict["GPU_memoryUtilization_average_per_node_gpu"]
        self.sm_clock_avg   =  dict["GPU_smClock_average_per_node_gpu"]
        self.sm_util_avg    =  dict["GPU_smUtilization_average_per_node_gpu"]
        self.energy_used    =  dict["GPU_energyConsumed_per_node_gpu"]
        self.gpu_duration   =  dict["GPU_duration_per_node_gpu"]
     
    def __repr__(self):
        return  self.node_name_and_gpu_number + ";" + \
                self.job_id + ";" + \
                self.mem_clock_avg + ";" + \
                self.mem_util_max + ";" + \
                self.mem_used_max + ";" + \
                self.mem_util_avg + ";" + \
                self.sm_clock_avg + ";" + \
                self.sm_util_avg + ";" + \
                self.energy_used + ";" + \
                self.gpu_duration



# A class for data parsing of the accounting logs
class Job():



    # The ID must be a valid pbs jobid and the data_dict should be a dictionary representation of the accounting log E record
    # associated with the job, with the Resource_List and resources_used stored as sub dictionaries  
    def __init__(self, id, data_dict):
        # Theses keys must be in the data_dict
        required_keys = [
                            'user', 'exec_host', 
                            'exec_vnode', 'group', 
                            'end', 'start', 'ctime', 
                            'qtime', 'etime', 
                            'Exit_status','exec_host', 
                            'queue', 'jobname', 
                            'session', 'run_count', 
                            'resources_used', 'Resource_List'
                           ]
    
        self.jobid = id
        # Checks for keys
        for key in required_keys:
            if key not in data_dict:
                raise KeyError("Missing " + key + " in data")
    
        self.data_dict = data_dict

    def get_data(self, tag):
        return self.data_dict[tag]

    def get_id(self):
        return self.jobid
    
    def get_resource_used(self, tag):
        return self.data_dict["resources_used"][tag]

    def get_resource_list(self, tag):
        return self.data_dict["Resource_List"][tag]
