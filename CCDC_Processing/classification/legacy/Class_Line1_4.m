function Class_Line1_4(dir_l,n_rst,nrow,model,num_c,nbands,anc,ntrees,yrs)
% This is the classification algorithm for classifying all map pixels by lines
%
% Revisions: $ Date: 04/06/2016 $ Copyright: Zhe Zhu
% Version 1.4  Classify value from current year (04/06/2016)
% Version 1.3  Add new disturbed Land Cover Trend class (04/05/2016)
% Version 1.2: Use Ancillary data from NLCD and Fmask (11/25/2015)
% Version 1.1: Combine classification and change detection output (07/05/2015)
% Version 1.0: Fast classification for each line (01/10/2015)

% fprintf('Processing the %d row\n',i);
load([dir_l,'/',n_rst,'/','record_change',num2str(nrow)]);

% total number of time series models (including empty ones)
num_ts =  length(rec_cg); %#ok<*NODEF>
% initiate Ids
IDs_rec = zeros(1,num_ts);
% number of classes
n_class = length(model.orig_labels);
% number of maps
n_yrs = length(yrs);
    
for i_model = 1:num_ts
    % record the time series ids that have recorded values
    % for adding classification results
    if ~isempty(rec_cg(i_model).pos)
        IDs_rec(i_model) = 1;
    end
end
    
% start time
t_start = [rec_cg.t_start];
% end time
t_end = [rec_cg.t_end];
% break time
t_break = [rec_cg.t_break];
% position
pos = [rec_cg.pos];
% rmse
rmse = [rec_cg.rmse]; % each curve has nbands rmse
% number of valid curves per line
num_s = sum(IDs_rec);
% number of bands for ancillary data
n_anc = size(anc,3);

if num_s > 0 % has more than one curves exist for each line
    
    % model coefficients
    tmp = [rec_cg.coefs];
    % prepare for classification inputs
    Xclass = zeros(num_s,(num_c+1)*(nbands-1) + n_anc);
    
    % array for storing ancillary data
    array_anc = zeros(n_anc,num_s);
    for i_a = 1:n_anc
        tmp_anc = anc(:,:,i_a);
        array_anc(i_a,:) = tmp_anc(pos);
    end
    
    % initialize map and votes
    map = zeros(n_yrs,num_s);
    votes = zeros(n_yrs,num_s,n_class);
    
    for i_yrs = 1:length(yrs)
        % record valid cuves
        id_vali = t_start <= yrs(i_yrs) & (t_end >= yrs(i_yrs) | t_break > yrs(i_yrs));
        for icol = 1:num_s;
            if id_vali(icol) == 1
                % coefficients from the 7 bands
                i_tmp = tmp(:,((icol-1)*(nbands-1)+1):(nbands-1)*icol);
                % modified constant as inputs
                i_tmp(1,:) = i_tmp(1,:)+yrs(i_yrs)*i_tmp(2,:);
                % input ready!
                Xclass(icol,:) = [reshape(rmse(((icol-1)*(nbands-1)+1):(nbands-1)*icol),nbands-1,1);i_tmp(:);array_anc(:,icol)];
            end
        end
        % classify the whole line
        [map(i_yrs,id_vali),votes(i_yrs,id_vali,:)] = classRF_predict(Xclass(id_vali,:),model,ntrees); % class
    end
    
    if sum(IDs_rec == 1) > 0
        IDs_add = find(IDs_rec == 1);
        for i = 1:length(IDs_add)
            rec_cg(IDs_add(i)).class = map(:,i);
            
            % largest number of votes
            [max_v1,max_id] = max(votes(:,i,:),[],3);
            % make this smallest
            votes(:,i,max_id) = 0;
            % second largest number of votes
            max_v2 = max(votes(:,i,:),[],3);
            % provide unsupervised ensemble margin as QA
            rec_cg(IDs_add(i)).classQA = 100*(max_v1-max_v2)/ntrees;
        end
    end
end

% add a new component "class" to rec_cg
if sum(IDs_rec == 0) > 0
    IDs_add = find(IDs_rec == 0);
    for i = 1:length(IDs_add)
        rec_cg(IDs_add(i)).class = [];
        rec_cg(IDs_add(i)).classQA = [];
    end
end

% updated "record_change*.mat"
save([dir_l,'/',n_rst,'/','record_change',num2str(nrow)],'rec_cg');
% end

end % end of the function

