# CPLSTool

        Detailed description of pipeline_config.txt
Example:

[Para]
Para_inputdir=/home/lusf/software/program/example
Para_outputdir=/home/lusf/software/CPLSTool/program/example/qiime_result
[DB]
DB_reference=/home/lusf/software/CPLSTool/program/example/97_otus.fasta
Description:
1.There are two main parts, [Para] and [DB], which are the definition of variables in the corresponding programs for the pipeline set by BIN_module.txt file. [Para] is representative of Parameters, and [DB] is representative of Database.
2.It is obvious that inputdir, outputdir and reference are the parameters set in the example of  BIN_module.txt file.
3. It's important to note that [sample] is another part, which can be set when the number of input samples are more than one, especially when the samples need to be analyzed one by one. The example is as the following:
  
(1)	The first column are experiment numbers of the samples;
(2)	The second column are the names of the samples,which will be used in the result;
(3)	The third column is the insert length of experiment libraries;
(4)	The fourth column is the sequencing length;
(5)	The fifth column is the base number of the samples;
(6)	The sixth column is quality control of the samples,such as raw data or clean data.
     
After the pipeline_config.txt file is finished, users can run the pipeline. There are more useful descriptions after running the pipeline.
1. If the minor step was broken, the process of all the pipeline would not be affected. But when users checked the show_process(which can be realized by the show_process.py program), the break status would be found. Users need to wait the accomplishment of the whole pipeline and reanalyzed the minor step again or modify the minor steps in shell scripts and deliver it manually.
2. If the major step was broken, the process of all the pipeline would certainly be affected, the job must be delivered again. Users need to kill all the false running tasks and deliver the whole job again.
3. The status of the tasks may be running, break, plan, end, running means the task is running, break means the task was broken, plan means the task is planning to run, end means the task is finished, hold means the storage of the disk is not enough and more storage is needed. If the status is break, users need to confirm which step was broken, if it is a major step, users need to do as the second description, if it is a minor step, users need to do as the first description. If the status is hold, or hqw, hr or ht are found when users use qstat to check the tasks, then users need to add DISK_QUOTA\t**G in the file sh.*.log, or kill the jobs and redeliver the jobs, users are advised to set a bigger storage to avoid the hold status at the beginning.
4. How to kill the tasks? If users want to kill all tasks, qdel is suitable; If users want to kill some tasks, users primarily need to confirm the ID by using ‘ps -f -u name |cat’, then use kill -9 to kill the chosen task.
