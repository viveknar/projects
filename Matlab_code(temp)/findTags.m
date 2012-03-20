%load('data_genre_identification/tagSongs1.mat');

tags = {};
for i = 1:size(songProperties1)
    tag = '';
    for j = 1:min(6,size(songProperties1{i}{8},1))
            tag = [tag, ', ',songProperties1{i}{8}{j}];
    end
    tags{i} = tag;
end

tags = tags';