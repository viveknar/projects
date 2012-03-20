
import sys
sys.path.append('PythonSrc')

import os
import glob
import hdf5_getters
import pymongo
import time

global parameters

def connect_song_db():
	connection = pymongo.Connection()	
	db = connection['song_db']
	song_db = db.song_db
	return song_db

parameters = dict(
		dataset_path = sys.argv[1],#'/home/vivek/datasets/MillionSongSubset/data/', 
		summaryfile_path = '/home/vivek/Desktop')
		

song = {}
def get_h5_files(base_dir, ext='.h5'):
	h5_files = []
	for dirpath, dirname, filenames in os.walk(base_dir):
		for filename in filenames:
			fileext = os.path.splitext(filename)
			if (fileext[1] == ext):
				try:
					h5_files.append(os.path.join(dirpath, filename))
					
				except IOError as e: 
					print os.path.join(dirpath, filename)+' not found'
	return h5_files

def variance(array):
	if(len(array) == 0):
		return 0

	m = mean(array)
	sum_of_diff = 0
	for i in range(len(array)):
		sum_of_diff += (array[i] - m)
	v = sum_of_diff**2/len(array)
	return v

def mean(array):
	if (len(array) == 0):
		return 0

	return sum(array)/len(array)

def split_segments(array2d):
	a0 = [];a1 = [];a2 = [];a3 = [];a4 = [];a5 = [];a6 = [];a7 = []
	a8 = [];a9 = [];a10 = [];a11 = []
	if (len(array2d) != 0):
		for j in range(len(array2d)):
			a0.append(array2d[j][0])
			a1.append(array2d[j][1])
			a2.append(array2d[j][2])
			a3.append(array2d[j][3])
			a4.append(array2d[j][4])
			a5.append(array2d[j][5])
			a6.append(array2d[j][6])
			a7.append(array2d[j][7])
			a8.append(array2d[j][8])
			a9.append(array2d[j][9])
			a10.append(array2d[j][10])
			a11.append(array2d[j][11])
	return (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)

def process_song(h5_song_file):
	song['artist_familiarity'] = hdf5_getters.get_artist_familiarity(h5)
	song['artist_id'] = hdf5_getters.get_artist_id(h5)
	song['artist_name'] = hdf5_getters.get_artist_name(h5)
	song['artist_hotttnesss'] = hdf5_getters.get_artist_hotttnesss(h5);
	song['title'] = hdf5_getters.get_title(h5)
	terms = hdf5_getters.get_artist_terms(h5)
	terms_freq = hdf5_getters.get_artist_terms_freq(h5)
	terms_weight = hdf5_getters.get_artist_terms_weight(h5)
	terms_array = []
	for i in range(len(terms)):
		terms_array.append([terms[i], terms_freq[i], terms_weight[i]])	
		
	song['artist_terms'] = terms_array
	beats_start = hdf5_getters.get_beats_start(h5)
	song['beats_start_variance'] = variance(beats_start)   #beats variance in yocto seconds(10^-24s)
	song['number_of_beats'] = len(beats_start)
	song['duration'] = hdf5_getters.get_duration(h5)
	song['loudness'] = hdf5_getters.get_loudness(h5)
	sections_start = hdf5_getters.get_sections_start(h5)
	song['sections_start_variance'] = variance(sections_start)
	song['number_of_sections'] = len(sections_start)
	
	segments_pitches = hdf5_getters.get_segments_pitches(h5)
	(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) = split_segments(segments_pitches)
	song['segments_pitches_variance'] = [variance(a0), variance(a1), variance(a2),
					variance(a3), variance(a4), variance(a5), variance(a6), variance(a7),
					variance(a8), variance(a9), variance(a10), variance(a11)]
	song['segments_pitches_mean'] = [mean(a0), mean(a1), mean(a2), mean(a3), mean(a4), 
					mean(a5), mean(a6), mean(a7), mean(a8), mean(a9), mean(a10), mean(a11)]
	
	segments_timbre = hdf5_getters.get_segments_timbre(h5)
	(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) = split_segments(segments_timbre)
	song['segments_timbre_variance'] = [variance(a0), variance(a1), variance(a2),
					variance(a3), variance(a4), variance(a5), variance(a6), variance(a7),
					variance(a8), variance(a9), variance(a10), variance(a11)]
	song['segments_timbre_mean'] = [mean(a0), mean(a1), mean(a2), mean(a3), mean(a4), 
					mean(a5), mean(a6), mean(a7), mean(a8), mean(a9), mean(a10), mean(a11)]
	song['tempo'] = hdf5_getters.get_tempo(h5)
	song['_id'] = hdf5_getters.get_song_id(h5)
	song['year'] = hdf5_getters.get_year(h5)	
	return song


if __name__ == '__main__':
	h5_files = get_h5_files(parameters['dataset_path'])
	song_db = connect_song_db()
	count = 0
	start_clock = time.time()
	for fname in h5_files:
		count += 1
		h5 = hdf5_getters.open_h5_file_read(fname)
		song = process_song(h5)
		song_db.insert(song)
		print count
		h5.close()
	end_clock = time.time()
	total_time = end_clock - start_clock
	print ('Done processing songs and adding them to database in {0} seconds', format(total_time))


# blah = songs.find({'number_of_beats':{'$gt':300}})
# for i in blah:
# 	print i


