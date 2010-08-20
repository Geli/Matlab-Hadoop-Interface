infile=getenv('MATLAB_MAPRED_INFIFO');
outfile=getenv('MATLAB_MAPRED_OUTFIFO');
if isempty(infile),
    fprintf('environment variable MATLAB_MAPRED_INFIFO not set\n');
    exit();
end
if isempty(outfile),
    fprintf('environment variable MATLAB_MAPRED_OUTFIFO not set\n');
    exit();
end
if ~exist(outfile,'file'),
    fprintf('out channel %s does not exist\n',outfile);
    exit();
end
if ~exist(infile,'file'),
    fprintf('in channel %s does not exist\n',infile);
    exit();
end
inid=fopen(infile,'r');
outid=fopen(outfile,'w');
while ~feof(inid),
    inkey=fullyread(inid,1,'uint32',0,'b');
    if feof(inid), %% we need to issue an additional read or feof will not fire
        break;
    end
    indatalen=fullyread(inid,1,'uint32',0,'b');
    indata=fullyread(inid,indatalen,'uint8',0,'b');
    [outkey,outdata]=mapper(inkey,indata);
    fwrite(outid,outkey,'uint32',0,'b');
    fwrite(outid,numel(outdata),'uint32',0,'b');
    fwrite(outid,outdata,'uint8',0,'b');
end
fclose(outid);
fclose(inid);