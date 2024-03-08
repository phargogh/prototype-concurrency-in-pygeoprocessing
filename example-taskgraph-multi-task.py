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
