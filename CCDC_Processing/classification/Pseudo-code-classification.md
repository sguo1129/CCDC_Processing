Classification - Training Part

1. Read in Land Cover Trend data (example_img) as reference or training data
2. Read in ancillary data from NLCD (slope, aspect, dem, position index (posidex), maximum potential wetland (mpw))
3. Read in Fmask ancillary data (Fmask_stat)
   Fmask_stat actually needs to be generated from Fmask result file
   Loop through all images:
   Add Fmask number of pixel with values of 0, 1, 3, 4 
   Add Fmask number of pixel with values less than 255
   Fmask_stat(:,:,1) = Fmask_stat(:,:,1) + double(im_Fmask == 0);
   Fmask_stat(:,:,2) = Fmask_stat(:,:,2) + double(im_Fmask == 1);
   Fmask_stat(:,:,3) = Fmask_stat(:,:,3) + double(im_Fmask == 3);
   Fmask_stat(:,:,4) = Fmask_stat(:,:,4) + double(im_Fmask == 4);
   All_stat = All_stat + double(im_Fmask < 255);
   Cloud probability: Fmask_stat(:,:,4) = 100*Fmask_stat(:,:,4)./All_stat;
   Snow probability:
      Fmask_stat(:,:,3) = 100*Fmask_stat(:,:,3)./(Fmask_stat(:,:,1)+Fmask_stat(:,:,2)+Fmask_stat(:,:,3)+0.01);
   Clear water probability
      Fmask_stat(:,:,2) = 100*Fmask_stat(:,:,2)./(Fmask_stat(:,:,1)+Fmask_stat(:,:,2)+0.01);
   Clear land probability
      Fmask_stat(:,:,1) = 100*Fmask_stat(:,:,1)./(Fmask_stat(:,:,1)+Fmask_stat(:,:,2)+0.01);

   
