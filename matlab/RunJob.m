function RunJob(job)
    jobfile=which(job);
    tmpdir=sprintf('%d',floor(now*1000));
    mkdir(tmpdir);
    curdir=pwd;
    cd(tmpdir);
    system(sprintf('mcc -m %s',jobfile));
    cd(curdir);
    rmdir(tmpdir,'s');