#!/alt/local/bin python

'''
 Converting Zhe's 4/14/2016 version of training code written in MatLAB to Python.
 This script contains three matlab codes: 'main_Train_ARDs.m', 'Merge_sample.m',
										 & 'main_Select_Train_Trends1_6_part2.m'
 Date: 7/12/2016
 Author: Devendra Dahal, EROS, USGS
 Version 0.0
 Based on: 
	 "Function for merging 8 neighborhood sample grids and train the center grid
	 Prepare data for Training_Strategy.m

	 CCDC 1.4 version - Zhe Zhu, EROS, USGS "	
Usage: Merge_ARD_samples_train_v0-1.py -help 
'''
 
import os, sys, traceback, time,subprocess, cPickle,scipy.io
import numpy as np
from datetime import date
import datetime as datetime
from optparse import OptionParser 
from sklearn.ensemble import RandomForestClassifier 
try:
	from osgeo import gdal
	from osgeo.gdalconst import *
except ImportError:
	import gdal	
	
print sys.version

t1 = datetime.datetime.now()
print t1.strftime("\n%Y-%m-%d %H:%M:%S\n")

## define GDAL raster format driver that will be used later on
##--------Start-----------
imDriver = gdal.GetDriverByName('ENVI')
imDriver.Register()

def ComputeRandomForest(X,Y, trees,FileDir, cngrid,n_times):
	print "......processing randomforest model fit for grid%s" % cngrid
	##number of times the area of a standard Landsat scene
	n_times = n_times*(25/37.0)
	# print n_times
	## only use the first column for training
	Y = Y[:,0]
	## remove disturbed classes 3 and 10 from Y
	'''
	ids_rm = np.nonzero((Y != 3) & (Y != 10)& (Y != 0))
	Y = Y[ids_rm]
	X = X[ids_rm]
	'''
	## number of variables
	x_dim = X.shape[0]
	
	## update class number
	all_class = np.unique(Y)
	
	## update number of class
	n_class = all_class.size
	
	## calculate proportion based # for each class
	prct,bin = np.histogram(Y,n_class)
	prct = prct/float(sum(prct))
	# print prct
	# sys.exit()
	## number of reference for euqal number training
	eq_num = np.ceil(20000*n_times) ## total # 
	n_min = np.ceil(600*n_times) ## minimum # 
	n_max = np.ceil(8000*n_times) ## maximum # 

	## intialized selected X & Y training data
	sel_X_trn = []
	sel_Y_trn = []
	for i_cls in range(0,n_class):
	
		## find ids for each land ocver class
		ids = np.where(Y == all_class[i_cls])
		# print (ids[0])
		## total # of referece pixels to permute
		tmp_N = ids[0].size
		# print tmp_N
		## random permute the reference pixels	
		tmp_rv = np.random.permutation(range(tmp_N))

		## adjust num_prop based on proportion
		adj_num = np.ceil(eq_num*prct[i_cls])
		
		## adjust num_prop based on min and max
		if adj_num < n_min:
			adj_num = n_min
		elif adj_num > n_max:
			adj_num = n_max
		
		if tmp_N > adj_num:
			tot_n = adj_num
		else:
			tot_n = tmp_N
		# print tmp_rv[1:int(tot_n)]
		# print ids
		## permutted ids
		rnd_ids = ids[0][tmp_rv[1:int(tot_n)]]

		# print Y[rnd_ids][0:10]
		
		## X_trn and Y_trn
		Y_rnd = Y[rnd_ids].tolist()
		X_rnd = X[rnd_ids,:].tolist()
		# sys.exit()
		
		sel_X_trn = sel_X_trn + X_rnd
		sel_Y_trn = sel_Y_trn + Y_rnd
					
	## log for CCDC Train paramters and versions
	## report only for the first task

	# print len(sel_X_trn)
	# print len(sel_Y_trn)
	modelRF = RandomForestClassifier(n_estimators = trees)
	modelRF = modelRF.fit(sel_X_trn,sel_Y_trn)
	
	### saving as out file
	RFfile = FileDir + os.sep + 'grid'+ cngrid + os.sep +'modelRF_py.dump'
	with open(RFfile, 'wb') as f:
		cPickle.dump(modelRF, f)
		
	### saving as .npy file for python	
	RFfile2 = FileDir + os.sep + 'grid'+ cngrid + os.sep + 'modelRF_py.npy'
	np.save(RFfile2, modelRF)   # X is an array
	
	'''
	### saving as .mat file for matlab	
	RFfile1 = FileDir + os.sep + 'modelRF_py.mat'
	h5f = h5py.File(RFfile1, 'w')
	h5f.create_dataset('modelRF', data=modelRF)
	# scipy.io.savemat(RFfile1,mydata)
	'''
	# print modelRF[0]
	# mod = modelRF.fit_transform(sel_X_trn,sel_Y_trn)
	# print mod.shape
	
	print 'model fit completed and 2 version of files saved:\n %s & %s......\n' %('modelRF_py.dump', 'modelRF_py.npy')

def Merge_Sample(cngrid,nb_grids,nfolder):
	
	# print cngrid,str(nb_grids[0])
	
	ingrid = str(nb_grids[0]).zfill(2)
	
	Target_X = nfolder + os.sep + 'grid'+ cngrid + os.sep + 'Xs_grid'+ cngrid+'.npy'
	Target_Y = nfolder + os.sep + 'grid'+ cngrid + os.sep + 'Ys_grid'+ cngrid+'.npy'
	
	if not os.path.exists(Target_X):
		print Target_X, 'not found, exiting!!\n\n'
		raise SystemExit	
	if not os.path.exists(Target_Y):
		print Target_Y, 'not found, exiting!!\n\n'
		raise SystemExit
		
	M_X = np.load(Target_X, mmap_mode = 'r')
	M_Y = np.load(Target_Y,mmap_mode = 'r')
	

	nb_grids.remove(int(cngrid))
	new_grid =[]
	for i, ng in enumerate(nb_grids):		
		ngrid =  str(ng).zfill(2)	
		print i, ng, ngrid		
		grid_Xs = nfolder + os.sep + 'grid'+ngrid + os.sep + 'Xs_grid'+ ngrid+'.npy'
		if os.path.exists(grid_Xs):
			print grid_Xs
			grid_Xs =  np.load(grid_Xs, mmap_mode = 'r')
			grid_Ys =  np.load(nfolder + os.sep + 'grid'+ngrid + os.sep + 'Ys_grid'+ ngrid+'.npy', mmap_mode = 'r')
			M_X = np.vstack((M_X,grid_Xs))
			M_Y = np.vstack((M_Y,grid_Ys))
			new_grid.append(ng)
		# else:
			
			# new_grid = nb_grids.remove(ng)
			# print new_grid
	XfileM = nfolder + os.sep + 'grid'+ cngrid + os.sep + 'Mrg_Xs_grid'+ cngrid+'.npy'
	YfileM = nfolder + os.sep + 'grid'+ cngrid + os.sep + 'Mrg_Ys_grid'+ cngrid+'.npy'
	
	np.save(XfileM, M_X)
	np.save(YfileM, M_Y)
	new_grid.append(int(cngrid))
	
	return M_X, M_Y, sorted(set(new_grid))

'''
This block will create a numpy array with id (CCRR: adding two digit Column and Row values) for weld tiles
if we decided to use the grid id itself instead of arbitrary ids
and find 8-connected neighborhood grids
'''
def newGrid():
	rows = range(0,21)
	cols = range(0,33)
	NewArray = np.zeros(shape=(21,33)).astype(np.uint16)
	print NewArray.shape
	for r in rows:
		rr = str(r).zfill(2)
		for c in cols:
			cc = str(c).zfill(2)
			val = int(cc+rr)
			NewArray[r,c] = val
	
	return NewArray
	
def allCalc(nfolder,tree):
	try:
			
		## Copying the 8-connected neighborhood grids
		grids = np.array([[2,3,4,5],
							[7,8,9,10],
							[11,12,13,14],
							[16,17,18,19]])
		# grids = newGrid()
		
		nr, nc = grids.shape
		t_size = grids.size

		## copying the selected ARD Xs & Ys
		for i in range(0,t_size):
			[row,col] = np.where(grids == grids.item(i))
			
			row = row[0]
			col = col[0]
			# print '\nrow %s, col %s' %(row, col)
			w_r = 1
			w_c = 1
			wr = 2
			wc = 2

			if (row - w_r) < 0:
				# print row - w_r,'..........'
				w_r = 0
			
			if (col - w_c) < 0:
				w_c = 0

			if (row + wr) > nr:
				wr = 1
			
			if (col + wc) > nc:
				wc = 1
			
			## neigbourhood grids
			nb_grids = grids[row-w_r:row+wr, col-w_c:col+wc]
			nb_grids =	np.array(nb_grids.reshape(-1).tolist()).reshape(-1).tolist()
			# print nb_grids
			
			## current grid in str
			cngrid =  str(grids.item(i)).zfill(2)
			
			print 'grid%s uses following grids for training ' % cngrid ,nb_grids
			
			## merge Xs & Ys from neighbouring folders 
			## This is translation of Merge_Sample.m
			X, Y,nb_grids = Merge_Sample(cngrid,nb_grids,nfolder)
			
			print nb_grids
			## number of grids used			
			gr_len = len(nb_grids)
			
			## Call defination to compute random forest
			## code of main_Select_Train_Trends##_part2.m 
			ComputeRandomForest(X,Y, tree,nfolder,cngrid,gr_len)
			
	except:
		print "Processed halted on the way."
		print traceback.format_exc()
def main():
	parser = OptionParser()

   # define options
	parser.add_option("-i", dest="in_Folder", help="(Required) Location of input data and place to save output")
	parser.add_option("-t", dest="ntrees", default = 500, help="number of trees, default is 500")
	# parser.add_option("-c", dest="num_cols", help="(Required) number of cols to get image dimension")
	(ops, arg) = parser.parse_args()

	if len(arg) == 1:
		parser.print_help()
	elif not ops.in_Folder:
		parser.print_help()
		sys.exit(1)
	# elif not ops.gt_start:
		# parser.print_help()
		# sys.exit(1)
	# elif not ops.gt_end:
		# parser.print_help()
		# sys.exit(1)

	else:
		allCalc(ops.in_Folder, ops.ntrees)  

if __name__ == '__main__':

	main()
	
t2 = datetime.datetime.now()
print t2.strftime("%Y-%m-%d %H:%M:%S")
tt = t2 - t1
print "\nProcessing time: " + str(tt) 