#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import salt 
from salt.cloud import CloudClient
from salt.cloud import Map


s3_key = os.environ['AWS_S3_KEY']
s3_secret = os.environ['AWS_S3_SECRET']


class MyMap(Map):
    def __init__(self, opts, defined_map):
        self.rendered_map = defined_map
        Map.__init__(self, opts)

        opts['hard'] = False
        opts['parallel'] = False

    def read(self):
        return self.rendered_map

    def destroy_map(self):
        matching = self.delete_map(query='list_nodes')
        names = set()
        for alias, drivers in matching.iteritems():
            for driver, vms in drivers.iteritems():
                for name in vms:
                    names.add(name)
        return self.destroy(names, cached=True)

    def create_map(self):
        dmap = self.map_data()
        return self.run_map(dmap)
 
 
spark_salt_conf = {
    "gitfs_remotes": [
        "https://github.com/oreh-formula/dnsmasq-formula.git",
        "https://github.com/oreh-formula/sysconfig-formula.git",
        "https://github.com/oreh-formula/lvm-formula.git",
        "https://github.com/wangqiang8511/spark_cluster.git",
        "https://github.com/wangqiang8511/sun-java-formula.git",
        "https://github.com/wangqiang8511/spark1-formula.git",
        "https://github.com/wangqiang8511/cdh5-hadoop2-formula.git",
        "https://github.com/wangqiang8511/mysql-formula.git",
        "https://github.com/wangqiang8511/hive-formula.git"
    ],
    "ext_pillar": [
        {"git": "master https://github.com/wangqiang8511/spark_cluster.git root=pillar"}
    ]
}


spark_master_minion_conf = {
    "grains": {
        "roles": {
            "salt": "syndic_master",
            "spark1": "master"
        },
        "salt": spark_salt_conf,
        "hadoop": {
            "master": "master",
            "set_s3_key": True,
            "s3_key": s3_key,
            "s3_secret": s3_secret,
            "data_dir": "/media/ephemeral0/hadoop"
        },
        "spark_env": {
            "java_home": "/usr/java/jdk1.8.0_05",
            "spark_home": "/var/spark",
            "spark_master_port": 7070,
            "spark_master_ip": "master",
            "spark_worker_cores": 1,
            "spark_worker_memory": "512m",
            "spark_worker_port": 9090,
            "spark_worker_instance": 1,
            "spark_local_dir": "/media/ephemeral0/spark/tmp"
        }
    }
}

spark_worker_minion_conf = {
    "grains": {
        "roles": {
            "salt": "syndic_minion",
            "spark1": "worker"
        },
        "salt":  spark_salt_conf,    
        "hadoop": {
            "master": "master",
            "set_s3_key": True,
            "s3_key": s3_key,
            "s3_secret": s3_secret,
            "data_dir": "/media/ephemeral0/hadoop"
        },
        "spark_env": {
            "java_home": "/usr/java/jdk1.8.0_05",
            "spark_home": "/var/spark",
            "spark_master_port": 7070,
            "spark_master_ip": "master",
            "spark_worker_cores": 1,
            "spark_worker_memory": "4999m",
            "spark_worker_port": 9090,
            "spark_worker_instance": 1,
            "spark_local_dir": "/media/ephemeral0/spark/tmp"
        }
    }
}

spark_map_basic =  {
    "image": "ami-4eb61926",
    "size": "m1.medium",
    "iam_profile": "rdp_admin_all",
}


def generate_map_file(cluster_name, number_of_workers, subnet, 
                      master_size='m1.medium', worker_size='m1.large'):
    spark_map = {}
    spark_map[subnet] = {}
    spark_master_name = "master.%s.razerbigdata.com" % cluster_name
    spark_map[subnet][spark_master_name] = spark_map_basic
    spark_map[subnet][spark_master_name]["name"] = spark_master_name
    spark_map[subnet][spark_master_name]["minion"] = spark_master_minion_conf
    spark_map[subnet][spark_master_name]["size"] = master_size
    for i in range(number_of_workers):
        spark_worker_name = "worker%d.%s.razerbigdata.com" % (i, cluster_name)
        spark_map[subnet][spark_worker_name] = spark_map_basic
        spark_map[subnet][spark_worker_name]["name"] = spark_worker_name
        spark_map[subnet][spark_worker_name]["minion"] = spark_worker_minion_conf
        spark_map[subnet][spark_worker_name]["size"] = worker_size
    return spark_map


def create_spark_cluster(cluster_name, number_of_workers, subnet, 
                      master_size='m1.medium', worker_size='m1.large'):
    c = CloudClient(path="/etc/salt/cloud")
    import salt.loader
    salt.loader.clouds(c._opts_defaults())
    opts = c.opts
    opts["keep_tmp"] = True
    t_spark_map = generate_map_file(cluster_name, number_of_workers,
                                   subnet, master_size, worker_size)
    m = MyMap(opts, t_spark_map)
    try:
        m.destroy_map()
    except:
        print "Nothing to destroy"
    print m.create_map()


def start_spark(cluster_name):
    local = salt.client.LocalClient("/etc/salt/master.d/master.conf")
    all_tag = "*.%s.razerbigdata.com" % cluster_name
    master_tag = "master.%s.razerbigdata.com" % cluster_name
    worker_tag = "worker*.%s.razerbigdata.com" % cluster_name

    all_ret = local.cmd(all_tag, "state.highstate")
    all_ret = local.cmd(all_tag, "state.sls", arg=("*", "network.ip_addrs"))
    all_ret = local.cmd(master_tag, "state.sls", arg=("hadoop2.format_dfs"))
    all_ret = local.cmd(master_tag, "state.sls", arg=("hadoop2.start_namenode"))
    all_ret = local.cmd(all_tag, "state.sls", arg=("hadoop2.start_datanode"))

    all_ret = local.cmd(master_tag, "state.sls", arg=("hadoop2.create_yarn_directories"))
    all_ret = local.cmd(master_tag, "state.sls", arg=("hadoop2.restart_resource_manager"))
    all_ret = local.cmd(master_tag, "state.sls", arg=("hadoop2.restart_jobhistory_server"))
    all_ret = local.cmd(all_tag, "state.sls", arg=("hadoop2.restart_node_manager"))

    all_ret = local.cmd(master_tag, "state.sls", arg=("spark1.start_master"))
    all_ret = local.cmd(all_tag, "state.sls", arg=("spark1.start_slave"))
    pass


def main():
    if not s3_key or not s3_secret:
        print "You need to setup AWS_S3_KEY, AWS_S3_SECRET to use this script"
    cluster_name = "sparktest"
    create_spark_cluster(cluster_name, 0, "3-us-east-1a-private")
    start_spark(cluster_name)


if __name__ == "__main__":
    main()
