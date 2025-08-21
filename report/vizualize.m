% MATLAB script to visualize raw SLAM trajectory and point cloud in 3D with large NED-aligned map axes
% Point cloud and trajectory in map frame (NED: X=North, Y=East, Z=Down)
% Tm2b: 4x4 transformation matrix from map (NED) to body frame
% Map axes explicitly aligned with NED frame, no frame adjustments
clear all; close all; clc;

% --- File Paths ---
pc_file = './traj/map_20250821_110030.txt'; % Point cloud file
traj_file = './traj/trajectory_20250821_110030.txt'; % Trajectory file

% --- Read Point Cloud ---
% Format: x y z (map frame, NED: X=North, Y=East, Z=Down)
try
    point_cloud = load(pc_file); % Nx3 matrix [x, y, z]
    if size(point_cloud, 2) ~= 3
        error('Point cloud file must have 3 columns (x, y, z). Found %d columns.', size(point_cloud, 2));
    end
    fprintf('Loaded %d points from point cloud file (NED frame).\n', size(point_cloud, 1));
catch e
    error('Failed to load point cloud file: %s', e.message);
end

% --- Read Trajectory ---
% Format: timestamp T(0,0)...T(3,3) w(0)...w(5) (23 columns)
% Tm2b: 4x4 transform from map (NED: X=North, Y=East, Z=Down) to body frame
try
    traj_data = load(traj_file);
    fprintf('Loaded %d trajectory points with %d columns.\n', size(traj_data, 1), size(traj_data, 2));
    if size(traj_data, 2) < 17
        error('Trajectory file has %d columns; expected at least 17 for Tm2b.', size(traj_data, 2));
    end
catch e
    error('Failed to load trajectory file: %s', e.message);
end

% --- Extract Trajectory Positions ---
n_traj = size(traj_data, 1);
positions = zeros(n_traj, 3); % [x, y, z] in map frame (NED)
for i = 1:n_traj
    % Extract 4x4 Tm2b (columns 2:17)
    Tm2b = reshape(traj_data(i, 2:17), [4, 4])';
    positions(i, :) = Tm2b(1:3, 4)'; % Body position in map frame (NED: X=North, Y=East, Z=Down)
end

% --- Create 3D Plot ---
figure('Name', 'Raw SLAM Trajectory and Point Cloud with Large NED Map Axes', 'Position', [100, 100, 1200, 800]);
hold on;

% Plot raw point cloud (no downsampling)
h_pc = scatter3(point_cloud(:, 1), point_cloud(:, 2), point_cloud(:, 3), 2, 'b.', ...
    'MarkerFaceAlpha', 0.3, 'DisplayName', 'Point Cloud (NED: X=North, Y=East, Z=Down)');
fprintf('Plotted %d points (raw, no downsampling).\n', size(point_cloud, 1));

% Plot raw trajectory
h_traj = plot3(positions(:, 1), positions(:, 2), positions(:, 3), 'r-', 'LineWidth', 2, ...
    'DisplayName', 'Trajectory (NED: X=North, Y=East, Z=Down)');

% Plot large map axes at origin, aligned with NED frame
map_axis_length = 10.0; % Large axis length (e.g., 10 meters for visibility)
h_map_axes = gobjects(1, 3);
% X-axis: North (+X), Red
h_map_axes(1) = quiver3(0, 0, 0, map_axis_length, 0, 0, 'r', 'LineWidth', 3, ...
    'MaxHeadSize', 0.8, 'AutoScale', 'off', 'DisplayName', 'Map X (North)');
% Y-axis: East (+Y), Green
h_map_axes(2) = quiver3(0, 0, 0, 0, map_axis_length, 0, 'g', 'LineWidth', 3, ...
    'MaxHeadSize', 0.8, 'AutoScale', 'off', 'DisplayName', 'Map Y (East)');
% Z-axis: Down (+Z), Blue
h_map_axes(3) = quiver3(0, 0, 0, 0, 0, map_axis_length, 'b', 'LineWidth', 3, ...
    'MaxHeadSize', 0.8, 'AutoScale', 'off', 'DisplayName', 'Map Z (Down)');

% --- Plot Settings ---
grid on;
axis equal; % Equal scaling to preserve raw data proportions
xlabel('X (North, m)');
ylabel('Y (East, m)');
zlabel('Z (Down, m)');
title('Raw SLAM Trajectory and Point Cloud with Large NED-Aligned Map Axes');

% Add legend
legend([h_pc, h_traj, h_map_axes], 'Location', 'best', 'AutoUpdate', 'off');

% Use default 3D view (no specific view angle)
% view(3); % MATLAB's default 3D view (azimuth=37.5, elevation=30)
% Keep MATLAB's default view without setting 'view'

hold off;

% --- Optional: Save Plot ---
% saveas(gcf, 'raw_slam_visualization_with_large_ned_map_axes.png');