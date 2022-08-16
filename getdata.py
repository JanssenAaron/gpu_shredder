from platform import node
import re
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from classes import Job, gpu_usage, sql_job
from classes import alchemy_class_sql_job, alchemy_class_gpu_usage
Base = declarative_base()



def _get_job_from_fields(id, fields):

    indexs=[]

    with open("fieldNames.txt", "r") as f:

        for field in f.readlines():

            index = fields.find(" "+field.strip())

            if(index==-1):
                index = (";"+fields).find(";"+field.strip())
                
            if(index==-1):
                continue

            index = index
            indexs.append(index)

        for match in re.finditer( r' resources_used\.\w+=', fields):

            indexs.append(match.start())

        for match in re.finditer( r' Resource_List\.\w+=', fields):

            indexs.append(match.start())

        indexs.sort()

        entries = []

        for i, j in zip(indexs, indexs[1:] +[None]):

            entries.append(fields[i:j])
        
        output = { "resources_used": {}, "Resource_List": {}}

        for entry in entries:

            tag, value = entry.split("=",1)
            tag = tag.strip()

            if tag.startswith("Resource_List") or tag.startswith("resources_used"):
                output[tag.split(".",1)[0]][tag.split(".",1)[1]] = value
                #if id == "1195277.bright01-thx" and "duration_per_node_gpu" in tag:

            else:
                output[tag] = value
        #if id == "1195277.bright01-thx":

        return Job(id, output)


def clock_speed_parser(string):
    return strip_n_from_values(string, 3)

def mem_used_parser(string):
    return strip_n_from_values(string, 2)

def energy_used_parser(string):
    return strip_n_from_values(string, 1)

def gpu_duration_parser(string):
    return strip_n_from_values(string, 3)

def util_parser(string):
    return strip_n_from_values(string, 1)


def strip_n_from_values(string, n):
    output = {}
    data = parse_gpu_per_node_stat(string)

    for key, value in data.items():
        output[key] = value[0:-n]
    return output

def parse_gpu_per_node_stat(string):

    output = {}
    nodes = string.split(")+")
    
    # Deals with an extra ")" on the strings
    for i, node in enumerate(nodes):
        node = node.strip()
        if node.endswith(")"):
            node = node[0:-1]
        nodes[i] = node

    for node in nodes:
        node_name, gpus= node.split(":(")

        for gpu in gpus.split("+"):
            gpu_num, gpu_stat = gpu.split(":")

            id_of_gpu = node_name+":"+gpu_num

            output[id_of_gpu] = gpu_stat
    
    return output

def get_jobs_from_file(filename):
    jobs = []
    with open(filename, "r") as f:
        for line in f.readlines():
            time, type, id, fields = line.split(';')
            if type != "E" or time.startswith("#"):
                continue
            job = _get_job_from_fields(id, fields)

            jobs.append(job)
    return jobs

def _sub_dict_parse(usage_dict, key, value, func):
    for usageid, stat in func(value).items():

        if usageid not in usage_dict.keys():
            usage_dict[usageid] = {}

        usage_dict[usageid][key] = stat
    return usage_dict

def get_sql_alchemy_objs_from_file(filename):

    jobs = get_jobs_from_file(filename)
    usage_objs = []
    job_objs = []
    for job in jobs:
        try:
            if job.get_resource_list("ngpus") != 0 and job.get_data("user") == "aaron.m.janssen" and "node0115" in job.get_data("exec_vnode"):
                id = job.get_id()
                
                used_resources = job.get_data("resources_used")

                usage_dict = {}
                for key, value in used_resources.items():
                    if key.startswith("GPU") and key in gpu_usage.valid_keys and "per_node_gpu" in key:

                        if "duration" in key:
                            _sub_dict_parse(usage_dict, key, value, gpu_duration_parser)
                        if "Clock" in key:
                            _sub_dict_parse(usage_dict, key, value, clock_speed_parser)
                        if "Utilization" in key:
                            _sub_dict_parse(usage_dict, key, value, util_parser)
                        if "MemoryUsed" in key:
                            _sub_dict_parse(usage_dict, key, value, mem_used_parser)
                        if "energy" in key:
                            _sub_dict_parse(usage_dict, key, value, energy_used_parser)
                for key in usage_dict.keys():
                    usage_dict[key]["jobid"] = id
                
                for node_gpu_name, data_dict in usage_dict.items():
                    obj = gpu_usage(node_gpu_name, data_dict)
                    usage_objs.append(obj)
                job_objs.append(sql_job(id, job))
        except Exception as e:
            print(str(e.with_traceback()))
    return (job_objs, usage_objs)

if __name__ == "__main__":
    jobs, usages = get_sql_alchemy_objs_from_file("testdata.txt")
    real_jobs = []
    real_usages = []
    for job in jobs:
        real_jobs.append(job.export_to_alchemy())
    for usage in usages:
        real_usages.append(usage.export_to_alchemy())

    engine = create_engine("mysql://root:password@10.244.1.89")
    engine.execute("use testjob")    

    #alchemy_class_gpu_usage.metadata.create_all(engine)
    #alchemy_class_sql_job.metadata.create_all(engine)    

    with Session(engine) as session:
        session.add_all(real_jobs)
        session.add_all(real_usages)
        session.commit()


    #engine.execute("Create Database testjob")


