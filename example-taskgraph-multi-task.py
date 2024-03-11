"""Parallelism with cores-per-task > 1 in taskgraph.
"""

import json
import logging
import os

import pygeoprocessing
import taskgraph

logging.basicConfig(level=logging.INFO)


def main():
    workspace = 'workspace_dir_multitask'
    with open('input_data.json') as input_json:
        data = json.load(input_json)
        data_files = data['data']
        bbox = data['bbox_stanford']

    # NOTE: This is a relatively simple example because:
    #   * All our input rasters are in the same projection
    #   * We know the target bounding box in advance
    #   * We aren't masking by another spatial layer
    #
    # If any of these conditions differed, we would need to build out the graph
    # to explicitly handle all of these cases that
    # `align_and_resize_raster_stack` just does for us for basically free.
    # Some of these calculations can be done without tasks (like warping a
    # bounding box), but others are best done with tasks (like creating a mask
    # raster and rasterizing a vector onto the new raster)

    graph = taskgraph.TaskGraph(os.path.join(workspace, '.taskgraph'),
                                n_workers=len(data_files))

    source_path_list = [
        f'/vsicurl/{url}' for url in data_files]
    target_path_list = [
        os.path.join(workspace, os.path.basename(url))
        for url in data_files]
    pixel_size = pygeoprocessing.get_raster_info(
        source_path_list[0])['pixel_size']

    # any other processing needed to compute the target bounding box,
    # pixel_size, etc.  would be done here before the loop.

    for source_path, dest_path in zip(source_path_list, target_path_list):
        graph.add_task(
            pygeoprocessing.warp_raster,
            kwargs=dict(
                base_raster_path=source_path,
                target_pixel_size=pixel_size,
                target_raster_path=dest_path,
                resample_method='near',
                target_bb=bbox,
            ),
            task_name=f'warp {source_path}',
            target_path_list=target_path_list,
        )

    graph.join()
    graph.close()


if __name__ == '__main__':
    main()
