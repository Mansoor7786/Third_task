import argparse
import unittest
from unittest.suite import TestSuite
from runner import execute
import subprocess

#creating the argument parser
parser = argparse.ArgumentParser()

parser.add_argument("--fs", help = "filesystem to mount",action='store')
parser.add_argument("--dtr", help = "Disk to remove", action='store')
parser.add_argument("--disk",nargs='+', help = "disk name", action='store')
parser.add_argument('--lvname', help="name of the logical volume",action='store')
parser.add_argument('--size', help="size of the lv",action='store')
parser.add_argument('--vgname', help="volume group name to allocate lv",action='store')
args = parser.parse_args()

#getting the values from argument parser
fs = args.fs
dtr = args.dtr
disk_name = args.disk
lvname = args.lvname
size = args.size
vgname = args.vgname


#loader to run the tests
if __name__ == '__main__':
    import test_file
    suite = TestSuite()
    loader = unittest.TestLoader()
    

    suite.addTests(loader.loadTestsFromTestCase(test_file.Task))
    #t1 = thread.Threading(target=
    runner = unittest.TextTestRunner()
    runner.run(suite)
    #unittest.main()

