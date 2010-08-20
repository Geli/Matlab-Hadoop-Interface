function data=fullyread(fid,numelems,precision,varargin)
    toread=numelems;
    totalread=0;
    data=zeros(numelems,1,precision);
    while toread>0,
        [buffer,numread]=fread(fid,toread,precision,varargin{:});
        if feof(fid),
            break;
        end
        data(totalread+(1:numread))=buffer;
        totalread=totalread+numread;
        toread=toread-numread;
    end