#!/usr/bin/env python
#-*- coding: utf-8 -*-


from salt.cloud import CloudClient
from salt.cloud import Map 


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
    'gitfs_remotes': [
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
    'ext_pillar': [
        {
            'git': "master https://github.com/wangqiang8511/spark_cluster.git root=pillar"
        }
    ]
}

spark_master_minion_conf = {
    'grains': {
        'roles': {
            'salt': 'syndic_master',
            'spark1': 'master'
        },
        'salt': spark_salt_conf,
        'hadoop': {
            'master': 'master',
            'data_dir': '/media/ephemeral0/hadoop'
        },
        'spark_env': {
            'java_home': '/usr/java/jdk1.8.0_05',
            'spark_home': '/var/spark',
            'spark_master_port': 7070,
            'spark_master_ip': 'master',
            'spark_worker_cores': 1,
            'spark_worker_memory': '512m',
            'spark_worker_port': 9090,
            'spark_worker_instance': 1
        }
    }
}

spark_worker_minion_conf = {
    'grains': {
        'roles': {
            'salt': 'syndic_minion',
            'spark1': 'worker'
        },
        'salt':  spark_salt_conf,
        'hadoop': {
            'master': 'master',
            'data_dir': '/media/ephemeral0/hadoop'
        },
        'spark_env': {
            'java_home': '/usr/java/jdk1.8.0_05',
            'spark_home': '/var/spark',
            'spark_master_port': 7070,
            'spark_master_ip': 'master',
            'spark_worker_cores': 1,
            'spark_worker_memory': '4999m',
            'spark_worker_port': 9090,
            'spark_worker_instance': 1
        }
    }
}


spark_map_basic =  {
    'image': 'ami-4eb61926',
    'size': 'm1.medium',
    'iam_profile': 'rdp_admin_all',
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


def main():
    c = CloudClient(path="/etc/salt/cloud")
    import salt.loader
    salt.loader.clouds(c._opts_defaults())
    opts = c.opts
    opts["keep_tmp"] = True
    spark_map = generate_map_file("sparktest", 0, "3-us-east-1a-private")
    m = MyMap(opts, spark_map)
    print m.interpolated_map()
    #m.destroy_map()
    print m.create_map()
    pass


if __name__ == "__main__":
    main()
