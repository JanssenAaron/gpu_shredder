import re
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

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

    def __repr__(self):
        return (self.job_id + ";" + self.ngpus + ";" + self.endtime + ";" + self.starttime)

class alchemy_class_gpu_usage(Base):
    __tablename__ = "gpuUsageStats"

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

class gpu_usage:

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

    def load_dict(self, dict):
        if len(dict) != 9:
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

class Job():
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
