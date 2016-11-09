Classification - Training Part

1. Read in Land Cover Trend data (example_img) as reference or training data
2. Read in ancillary data from NLCD (slope, aspect, dem, position index (posidex), maximum potential wetland (mpw))
3. Read in Fmask ancillary data (Fmask_stat)
   1. Fmask_stat actually needs to be generated from Fmask result file
   2. Loop through all images:
   3. Add Fmask number of pixel with values of 0, 1, 3, 4 
   4. Add Fmask number of pixel with values less than 255
   5. Fmask_stat(:,:,1) = Fmask_stat(:,:,1) + double(im_Fmask == 0);
   6. Fmask_stat(:,:,2) = Fmask_stat(:,:,2) + double(im_Fmask == 1);
   7. Fmask_stat(:,:,3) = Fmask_stat(:,:,3) + double(im_Fmask == 3);
   8. Fmask_stat(:,:,4) = Fmask_stat(:,:,4) + double(im_Fmask == 4);
   9. All_stat = All_stat + double(im_Fmask < 255);
   10. Cloud probability: Fmask_stat(:,:,4) = 100*Fmask_stat(:,:,4)./All_stat;
   11. Snow probability:
      12. Fmask_stat(:,:,3) = 100*Fmask_stat(:,:,3)./(Fmask_stat(:,:,1)+Fmask_stat(:,:,2)+Fmask_stat(:,:,3)+0.01);
   13. Clear water probability
      14. Fmask_stat(:,:,2) = 100*Fmask_stat(:,:,2)./(Fmask_stat(:,:,1)+Fmask_stat(:,:,2)+0.01);
   15. Clear land probability
      16. Fmask_stat(:,:,1) = 100*Fmask_stat(:,:,1)./(Fmask_stat(:,:,1)+Fmask_stat(:,:,2)+0.01);
4. Finf non-zero pixels and label them with IDs
5. Initialize row number counter (i_row) to be -1
6. Initialize training pixel counter (plusid) to be 0
7. Loop through non-zero land cover trend pixels
   1. If the pixel is not equal to row number counter i_row
       Assembles all CCD outputs and prepare them as part of inputs for RF training
   2. Find the curve within a fixed time interval produced by CCD
   3. If no pixel found within the time interval
      continue
   4. Else
      1. Loop through the pixels within the fixed time interval 
      2. Take curves that fall within the training period & remove curves that are changed within the training period
      3. tmp_cft(1,:) = tmp_cft(1,:)+gt_end*tmp_cft(2,:);
      4. Assembles all NLCD ancillary data & Fmask statistics & CCD outputs and put them as X input for RF training
      5. Put land cover trend reference data as Y input for RF training
      5. Put all non-zero land cover trend pixel 
      6. Increase training pixel counter plusid by 1
   5. Set row number counter to be the current row number
8. remove out of boundary or changed pixels  
9. Output RF training input X and Y arrays into output files is an option
10. Remove disturbed classes (values of 3 and 5) and zero value pixels in land cover trend reference data
11. Calculate histogram bin or number of pixels for each land cover class
12. Calculate percentage of each class cover bin to the total class cover bins (prct)
13. Set time interval of a standard Landsat scene n_times to be (25/37)*number of grids used	
14. Set number of reference for euqal number training
    1. eq_num = ceil(20000*n_times); % total %
    2. n_min = ceil(600*n_times); % minimum %
    3. n_max = ceil(8000*n_times); % maximum % 
15. Intialized selected X & Y training data by looping through number of classes
    1. Find ids for each land cover class
    2. Generate random permutation of the integer from 1 to length(ids)
    3. Adjust num_prop based (adj_num) on prct (adj_num = ceil(eq_num*prct(i_class));)
    4. Adjust num_prop  based (adj_num) on n_min & n_max
    5. if length(ids) > adj_num
         Use adj_num to select reference pixels for training
    Else
         Use all ids for this land cover class
16. Log for CCDC training parameters and versions and report only for the first task (optional)
17. Random Forest model training uses X & Y arrays as inputs with default 500 regression trees
18. Save RF model output structure into a file or into database in the future

      
      
   



      
      

   
