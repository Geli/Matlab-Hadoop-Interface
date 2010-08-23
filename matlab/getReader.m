function reader=getReader(type,isArray)
    if nargin<2
        isArray=false;
    end
    
    function data = readArray(fid,type)
        len=fread(fid,1,'uint32',0,'b');
        data=fread(fid,len,type,0,'b');
    end
    
    switch type
        case {'uint8','uint16','uint32','uint64','uchar','int','int8',...
                'int16','int32','int64','integer*1','integer*2','integer*3',...
                'integer*4','schar','signed char','short','long','single',...
                'double','float','float32','float64','real*4','real*8'}
            if isArray,
                reader=@(fid) readArray(fid,type);
            else
                reader=@(fid) fread(fid,1,type,0,'b');
            end
        case 'char'
            reader=@(fid) readArray(fid,type);
        otherwise
            error('unknown type %s',type);
    end
end