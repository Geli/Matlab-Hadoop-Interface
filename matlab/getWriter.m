function writer=getWriter(type,isArray)
    if nargin<2
        isArray=false;
    end

    function writeArray(fid,data,type)
        fwrite(fid,numel(data),'uint32',0,'b');
        fwrite(fid,data,type,0,'b');
    end
    
    switch type
        case {'uint8','uint16','uint32','uint64','uchar','int','int8',...
                'int16','int32','int64','integer*1','integer*2','integer*3',...
                'integer*4','schar','signed char','short','long','single',...
                'double','float','float32','float64','real*4','real*8'}
            if isArray,
                writer=@(fid,data) writeArray(fid,data,type);
            else
                writer=@(fid,data) fwrite(fid,data,type,0,'b');
            end
        case 'char'
            writer=@(fid,data) writeArray(fid,data,type);
        otherwise
            error('unknown type %s',type);
    end
end