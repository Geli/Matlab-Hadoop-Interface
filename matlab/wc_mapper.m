function [outkey,outdata]=wc_mapper(~,indata)
    splitpos=[0,find(indata==' '),numel(indata)+1];
    n=numel(splitpos)-1;
    outdata=num2cell(ones(n,1));
    outkey=cell(n,1);
    for i=1:n
        outkey{i}=indata((splitpos(i)+1):splitpos(i+1)-1);
    end