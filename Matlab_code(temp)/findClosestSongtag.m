function tag = findClosestSongtag(group, song, tags)

[m,n] = size(group);
dist = zeros(m,1);
for i = 1:m
   dist(i) = computeDistance(song, group(i,:));
   [d, id] = min(dist);
end

tag = tags(id);
