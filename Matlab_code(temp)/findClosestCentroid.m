function idx = findClosestCentroid(input, centroid)

M = size(input, 1);
K = size(centroid, 1);

idx = zeros(M, 1);
distances = zeros(K,1);
for i = 1:M
    for j = 1:K
        distances(j) = computeDistance(input(i,:), centroid(j,:));
    end
    [point, cluster] = min(distances);
    idx(i) = cluster;
end