function [centroids] = computeCentroids(input, idx, K)
centroids = zeros(K,size(input,2));

sum = zeros(size(input));
count = zeros(K,1);

for i = 1:size(input)
   sum(idx(i), :) = sum(idx(i), :) + input(i, :);
   count(idx(i)) = count(idx(i)) + 1;
end

for i = 1:K
    centroids(i,:) = sum(i,:) ./ count(i,1);
end

