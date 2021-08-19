import unittest
import threading
import cli
from test import support
import time
from runner import execute,fio


class Task(unittest.TestCase):
    disk = " ".join(cli.disk_name)

    @classmethod
    def setUpClass(cls):
        print("Step 1: Creating Physical Volume {}".format(Task.disk))
        execute("pvcreate {}" .format(Task.disk))

        print("Step 2: Creating Volume group {}".format(cli.vgname))
        execute("vgcreate {} {}" .format(cli.vgname, Task.disk))

        print("Step 3: Creating Logical Volume {}" .format(cli.lvname))
        execute("lvcreate --size {} --name {} {}".format(cli.size,cli.lvname,cli.vgname))

        cls.lvpath = "/dev/{}/{}" .format(cli.vgname, cli.lvname)

        print("Step 4: Creating an {} filesystem on the Logical Volume {}" .format(cli.fs,cli.lvname))
        execute("sudo mkfs -t {} {}" .format(cli.fs, cls.lvpath))
        
        print("Step 5: Creating a directory named data under root directory for mounting the logical volume")
        execute("mkdir /data")

        print("Step 6: Mounting the /data directory")
        execute("mount {} /data" .format(cls.lvpath))


    @classmethod
    def tearDownClass(cls):
        #destroying pv,vg and lv ater running the test

        cls.vgpath = "/dev/{}/{}".format(cli.vgname,cli.lvname)
        print("\nUnmounting /data directory")
        execute("umount /data")

        print("Removing /data directory")
        execute("rmdir /data")

        print("Wiping the filesystem from the Logical Volume")
        execute("wipefs -a {}".format(cls.vgpath))

        print("Removing the Logical Volume {}".format(cli.lvname))
        execute("lvremove -f {}" .format(cli.vgname), inp="y\n")

        print("Removing the Volume Group {}".format(cli.vgname))
        execute("vgremove {}".format(cli.vgname))
        
        print("Removing the Physical Volume {}".format(Task.disk))
        execute("pvremove {}" .format(Task.disk))



    def hread(self):
        time.sleep(3)
        execute("pvmove {}".format(cli.dtr))
        execute("vgreduce {0} {1}" .format(cli.vgname,cli.dtr))
        self.out = execute("pvdisplay -C -o pv_name,vg_name -S vgname={}".format(cli.vgname))
        self.assertNotIn(cli.dtr,self.out)
        print("vgreduced")



    def test_th(self):
        self.lvpath="/dev/{}/{}".format(cli.vgname,cli.lvname)

        self.cmd = "fio --filename={} --direct=1 --size=1G --rw=randrw --bs=4k --ioengine=libaio --iodepth=256 --runtime=5 --numjobs=32 --time_based --group_reporting --name=iops-test-job --allow_mounted_write=1".format(self.lvpath)
        
        with support.catch_threading_exception() as cm:
            t1=threading.Thread(target=fio, args=(self.cmd,))
            t2=threading.Thread(target=self.hread)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            if cm.exc_value is not None:
                raise cm.exc_value


