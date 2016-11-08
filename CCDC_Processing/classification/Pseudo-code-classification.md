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

      
      

   
