import unittest
import threading
import cli
from test import support
import time
from runner import execute,fio
import subprocess

class Task(unittest.TestCase):
    disk = " ".join(cli.disk_name)
    
    @classmethod 
    def tearDownClass(cls):
        #destroying pv ater running the test
        disk = " ".join(cli.disk_name)
        
        vgpath = "/dev/{}/{}".format(cli.vgname,cli.lvname)
        print("\nUnmounting /data directory")
        execute("umount /data")

        print("Removing /data directory")
        execute("rmdir /data")

        print("Wiping the filesystem from the Logical Volume")
        execute("wipefs -a {}".format(vgpath))

        print("Removing the Logical Volume {}".format(cli.lvname))
        execute("lvremove -f {}" .format(cli.vgname), inp="y\n")

        print("Removing the Volume Group {}".format(cli.vgname))
        execute("vgremove {}".format(cli.vgname))
        
        print("Removing the Physical Volume {}".format(Task.disk))
        execute("pvremove {}" .format(disk))


    def xlvcreate(self):
        print("Step 1: Creating Physical Volume {}".format(Task.disk))
        execute("pvcreate {}" .format(Task.disk))

        print("Step 2: Creating Volume group {}".format(cli.vgname))
        execute("vgcreate {} {}" .format(cli.vgname, Task.disk))

        print("Step 3: Creating Logical Volume {}" .format(cli.lvname))
        execute("lvcreate --size {} --name {} {}".format(cli.size,cli.lvname,cli.vgname))
        
        self.lvpath = "/dev/{}/{}" .format(cli.vgname, cli.lvname)
        
        print("Step 4: Creating an {} filesystem on the Logical Volume {}" .format(cli.fs,cli.lvname))
        execute("sudo mkfs -t {} {}" .format(cli.fs, self.lvpath))

        print("Step 5: Creating a directory named data under root directory for mounting the logical volume")
        execute("mkdir /data")

        print("Step 6: Mounting the /data directory")
        execute("mount {} /data" .format(self.lvpath))
        
        print("Step 7: Performing IO using fio")
        self.fio_fun = fio("fio --filename={} --direct=1 --size=1G --rw=randrw --bs=4k --ioengine=libaio --iodepth=256 --runtime=5 --numjobs=32 --time_based --group_reporting --name=iops-test-job --allow_mounted_write=1".format(self.lvpath))
        self.fspath = "dev/mapper/{}-{}" .format(cli.vgname, cli.lvname)
        self.outpv = execute("pvdisplay")
        self.outvg = execute("vgdisplay")
        self.output = execute("lvdisplay")
        self.outmnt = execute("findmnt")
        
        for i in Task.disk:
            self.assertIn(i,self.outpv)
        self.assertIn(cli.vgname,self.outvg)
        self.assertIn(cli.lvname, self.output)
        self.assertIn(self.fspath,self.outmnt)
        self.assertIn("Run status",self.fio_fun)


    def hread(self):
        time.sleep(3)
        execute("pvmove {}".format(cli.dtr))
        execute("vgreduce {0} {1}" .format(cli.vgname,cli.dtr))
        self.out = execute("pvdisplay -C -o pv_name,vg_name -S vgname={}".format(cli.vgname))
        self.assertNotIn(cli.dtr,self.out)
        print("vgreduced")


    def test_th(self):
        with support.catch_threading_exception() as cm:
            t1=threading.Thread(target=self.xlvcreate)
            t2=threading.Thread(target=self.hread)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            if cm.exc_value is not None:
                raise cm.exc_value

            

