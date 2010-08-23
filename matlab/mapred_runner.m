s=dir('/tmp/Matlab*');


setenv('MATLAB_MAPRED_INFIFO',sprintf('/tmp/%s.in',s(1).name));
setenv('MATLAB_MAPRED_OUTFIFO',sprintf('/tmp/%s.out',s(1).name));

mapred_mapper_runner(@mapper,getReader('uint32'),getReader('uint8',true),getWriter('uint32'),getWriter('uint8',true))