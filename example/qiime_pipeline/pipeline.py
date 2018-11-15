
'''
Parameters:
     -i , --input : The pipeline_config.txt file
     -b,--bin: The BIN directory, which corresponds to ${BIN} in the configuration file, BIN_module.txt. It's important to note that all the tools and programs must be put in the same directory.
     -t , --thread: Number of threads, if it is set, the number of threads in the BIN_module.txt
file will be covered.
-q , --queue : Queue name, if it is set, the queue name in the BIN_module.txt
file will be covered.
     -o ,--outdir: The output directory, which corresponds to the $(OUTDIR) in the configuration file, BIN_module.txt.
-name,--name : The name of the job, it must be set.
     -j,   --jobid: The prefix of the job, it is acquiescently same as the name of the job.
     -r,   --run : Dose the job be delivered by itself? The default value is not, if not, users can check the scripts in the directory ${OUTDIR}/shell before operating the jobs.
     -c,   --continue: It is available only when qsub is used(-r is set and –n is not). If it is set, the remaining unfinished tasks are needed to be analyzed when breaking. If not, all the tasks are needed to be analyzed when breaking.
     -n,   --nohup : If it is set, nohup will be used to operate the jobs; If not, qsub will be used to operate the jobs. The default value of it is qsub.
     -a,   --add   : If it is set, only the additional samples will be analyzed. If not, all the input samples will be analyzed. What is additional samples? It means that when the base number of the sample can not meet the requirement, more bases will be sequenced again.
     -quota, --quota : The limit storage of the result directory. The default value is 1000G.

'''
#! /usr/bin/env python3
# -*- coding: utf-8 -*-  
import argparse
import sys
import os
import re
bindir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('{0}/lib'.format(bindir))
import parseConfig
import JobGuard

__author__='Lusifen'
__mail__= 'sifenlu2017@163.com'

pat1=re.compile('^\s+$')

def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\t{0}\nmail:\t{1}'.format(__author__,__mail__))
	parser.add_argument('-i','--input',help='input file',dest='input',type=open,required=True)
	parser.add_argument('-b','--bin',help='bin dir',dest='bin',default=os.path.dirname(bindir))
	parser.add_argument('-t','--thread',help='thread number ',dest='thread',type=int)
	parser.add_argument('-q','--queue',help='computer queue name ',dest='queue')
	parser.add_argument('-o','--outdir',help='output file',dest='outdir',required=True)
	parser.add_argument('-name','--name',help='project name',dest='name',required=True)
	parser.add_argument('-j','--jobid',help='job id prefix',dest='jobid',default='')
	parser.add_argument('-r','--run',help='run script file',dest='run',action='store_true')
	parser.add_argument('-c','--continue',help='continue unfinish job in each shell',dest='continues',action='store_true')
	#parser.add_argument('-n','--nohup',help='qsub or nohup mission',dest='nohup',action='store_true')
	parser.add_argument('-quota','--quota',help='disk quota ',dest='quota',default = '1000G')
	parser.add_argument('-a','--add',help='add sequencing sample process, True -- only run added sequence sample ,False* -- run all samples',dest='add',action='store_true')
	args=parser.parse_args()

	OUTDIR = parseConfig.getab(args.outdir)
	BIN=os.path.realpath(args.bin)
	LOGFILE = '{0}/log.txt'.format(OUTDIR)
	
	job_not_continue = ' -nc '
	if args.continues : job_not_continue = ' '

	if args.jobid == '' : args.jobid = args.name
	config ,para, db , orders  = parseConfig.ReadConfig(args.input)
	shell_dir = '{0}/shell/'.format(OUTDIR)
	parseConfig.makedir(shell_dir)
	logfile = '{0}/log.txt'.format(shell_dir)
	finish_obj = JobGuard.ReadLog(logfile)
	log = open(logfile,'a')
	log.write('#pipeline version : {0}\n'.format(BIN))
	guard_script = '{0}/guard.py'.format(shell_dir)
	job_list = {} 

	shsh = '{0}/1_0_pickotu.sh'.format(shell_dir)
	with open(shsh , 'w') as f_out:
		cmds = []
		cpu = parseConfig.cpu(args.thread , 1 , 'N' )
		queue = parseConfig.queue(args.queue , 'sci.q')
		cmds.append('{BIN}/pick_open_reference_otus.py -i {para[Para_inputdir]}/skin-annuo.fa -r {db[DB_reference]} -o {para[Para_outputdir]} -f'.format( para = para, OUTDIR = OUTDIR , BIN = BIN , db = db , LOGFILE = LOGFILE ))
		f_out.write('\n'.join(set(cmds)))
	if not False :
		a_cmd = 'perl {1}/src/multi-process.pl -cpu {2} --lines 1 {0}'.format(shsh , bindir, cpu)
	else:
		a_cmd = 'python3 {1}/src/qsub_sge.py --resource "p=1 -l vf=2G" --maxjob {2}  --lines 1 --jobprefix {3}pickotu  {5} --queue {4} {0}'.format(shsh , bindir, cpu , args.jobid , queue , job_not_continue)
	a_thread = JobGuard.MyThread('1_0_pickotu' , log , a_cmd, True)
	if not int(1) in job_list: job_list[int(1)] = []
	job_list[int(1)].append(a_thread)

	shsh = '{0}/2_0_biom2table.sh'.format(shell_dir)
	with open(shsh , 'w') as f_out:
		cmds = []
		cpu = parseConfig.cpu(args.thread , 1 , 'N' )
		queue = parseConfig.queue(args.queue , 'sci.q')
		cmds.append('{BIN}/biom convert --header-key taxonomy --to-tsv -i {para[Para_inputdir]}/otu_table_mc2_w_tax_no_pynast_failures.biom -o {para[Para_outputdir]}/otu_table.txt'.format( para = para, OUTDIR = OUTDIR , BIN = BIN , db = db , LOGFILE = LOGFILE ))
		f_out.write('\n'.join(set(cmds)))
	if not False :
		a_cmd = 'perl {1}/src/multi-process.pl -cpu {2} --lines 1 {0}'.format(shsh , bindir, cpu)
	else:
		a_cmd = 'python3 {1}/src/qsub_sge.py --resource "p=1 -l vf=2G" --maxjob {2}  --lines 1 --jobprefix {3}biom2table  {5} --queue {4} {0}'.format(shsh , bindir, cpu , args.jobid , queue , job_not_continue)
	a_thread = JobGuard.MyThread('2_0_biom2table' , log , a_cmd, True)
	if not int(2) in job_list: job_list[int(2)] = []
	job_list[int(2)].append(a_thread)

	shsh = '{0}/3_0_level.sh'.format(shell_dir)
	with open(shsh , 'w') as f_out:
		cmds = []
		cpu = parseConfig.cpu(args.thread , 1 , 'N' )
		queue = parseConfig.queue(args.queue , 'sci.q')
		cmds.append('{BIN}/summarize_taxa_through_plots.py -i {para[Para_outputdir]}/otu_table_mc2_w_tax_no_pynast_failures.biom -o {para[Para_outputdir]}/taxa_summaries'.format( para = para, OUTDIR = OUTDIR , BIN = BIN , db = db , LOGFILE = LOGFILE ))
		f_out.write('\n'.join(set(cmds)))
	if not False :
		a_cmd = 'perl {1}/src/multi-process.pl -cpu {2} --lines 1 {0}'.format(shsh , bindir, cpu)
	else:
		a_cmd = 'python3 {1}/src/qsub_sge.py --resource "p=1 -l vf=2G" --maxjob {2}  --lines 1 --jobprefix {3}level  {5} --queue {4} {0}'.format(shsh , bindir, cpu , args.jobid , queue , job_not_continue)
	a_thread = JobGuard.MyThread('3_0_level' , log , a_cmd, True)
	if not int(3) in job_list: job_list[int(3)] = []
	job_list[int(3)].append(a_thread)

	shsh = '{0}/3_1_alphadiversity.sh'.format(shell_dir)
	with open(shsh , 'w') as f_out:
		cmds = []
		cpu = parseConfig.cpu(args.thread , 1 , 'N' )
		queue = parseConfig.queue(args.queue , 'sci.q')
		cmds.append('{BIN}/parallel_alpha_diversity.py -i {para[Para_outputdir]}/otu_table_mc2_w_tax_no_pynast_failures.biom -o alpha -m chao1,observed_species,shannon,simpson -t rep_set.tre'.format( para = para, OUTDIR = OUTDIR , BIN = BIN , db = db , LOGFILE = LOGFILE ))
		f_out.write('\n'.join(set(cmds)))
	if not False :
		a_cmd = 'perl {1}/src/multi-process.pl -cpu {2} --lines 1 {0}'.format(shsh , bindir, cpu)
	else:
		a_cmd = 'python3 {1}/src/qsub_sge.py --resource "p=1 -l vf=2G" --maxjob {2}  --lines 1 --jobprefix {3}alphadiversity  {5} --queue {4} {0}'.format(shsh , bindir, cpu , args.jobid , queue , job_not_continue)
	a_thread = JobGuard.MyThread('3_1_alphadiversity' , log , a_cmd, False)
	if not int(3) in job_list: job_list[int(3)] = []
	job_list[int(3)].append(a_thread)

	shsh = '{0}/3_2_betadiversity.sh'.format(shell_dir)
	with open(shsh , 'w') as f_out:
		cmds = []
		cpu = parseConfig.cpu(args.thread , 1 , 'N' )
		queue = parseConfig.queue(args.queue , 'sci.q')
		cmds.append('{BIN}/parallel_beta_diversity.py -i {para[Para_outputdir]}/otu_table_mc2_w_tax_no_pynast_failures.biom -o beta -t rep_set.tre -m bray_curtis,weighted_unifrac, unweighted_unifrac'.format( para = para, OUTDIR = OUTDIR , BIN = BIN , db = db , LOGFILE = LOGFILE ))
		f_out.write('\n'.join(set(cmds)))
	if not False :
		a_cmd = 'perl {1}/src/multi-process.pl -cpu {2} --lines 1 {0}'.format(shsh , bindir, cpu)
	else:
		a_cmd = 'python3 {1}/src/qsub_sge.py --resource "p=1 -l vf=2G" --maxjob {2}  --lines 1 --jobprefix {3}betadiversity  {5} --queue {4} {0}'.format(shsh , bindir, cpu , args.jobid , queue , job_not_continue)
	a_thread = JobGuard.MyThread('3_2_betadiversity' , log , a_cmd, False)
	if not int(3) in job_list: job_list[int(3)] = []
	job_list[int(3)].append(a_thread)

	home_dir = os.environ['HOME']
	parseConfig.makedir('{0}/.mission'.format(home_dir))
	if not os.path.isfile('{0}/.mission/.pipeline.log'.format(home_dir)):
		os.system('touch {0}/.mission/.pipeline.log'.format(home_dir))
	
	tag = 0 
	with open('{0}/.mission/.pipeline.log'.format(home_dir),'r') as super_log:
		for line in super_log:
			if line.startswith('#') or re.search(pat1,line):continue
			tmp = line.rstrip().split()
			if tmp[0] == args.name:
				tag = 1
				if tmp[1] == os.path.abspath(args.outdir):
					tag = 2
	
	if tag == 0 : 
		with open('{0}/.mission/.pipeline.log'.format(home_dir),'a') as super_log:
			super_log.write('{0}\t{1}\n'.format(args.name , os.path.abspath(args.outdir)))
	elif tag == 2 :
		print("[1;31;40m" + "Warings: {0} was existed already in your log file, please check it".format(args.name) + "[0m")
	elif tag == 1 :
		print("[1;31;40m" + "Warings: {0} was existed already in your log file,  and have different analysis directory , we should add this new dir at the end of log file ,please check it".format(args.name) + "[0m")
		with open('{0}/.mission/.pipeline.log'.format(home_dir),'a') as super_log:
			super_log.write('{0}\t{1}\n'.format(args.name , os.path.abspath(args.outdir)))
	
	job_list = JobGuard.RemoveFinish(job_list,finish_obj)
	if args.run == True:
		JobGuard.run(job_list)

if __name__ == '__main__':
	main()
