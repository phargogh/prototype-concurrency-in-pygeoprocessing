"""Parallelism with cores-per-task > 1 in taskgraph.
"""

import json
import logging
import os

import pygeoprocessing
import taskgraph

logging.basicConfig(level=logging.INFO)


def main():
    workspace = 'workspace_dir_singletask'
    with open('input_data.json') as input_json:
        data = json.load(input_json)
        data_files = data['data']
        bbox = data['bbox_stanford']

    graph = taskgraph.TaskGraph(os.path.join(workspace, '.taskgraph'),
                                n_workers=-1)  # task creates subprocesses

    source_path_list = [
        f'/vsicurl/{url}' for url in data_files]
    target_path_list = [
        os.path.join(workspace, os.path.basename(url))
        for url in data_files]
    pixel_size = pygeoprocessing.get_raster_info(
        source_path_list[0])['pixel_size']
    graph.add_task(
        pygeoprocessing.align_and_resize_raster_stack,
        kwargs=dict(
            base_raster_path_list=source_path_list,
            target_raster_path_list=target_path_list,
            resample_method_list=['near'] * len(source_path_list),
            target_pixel_size=pixel_size,
            bounding_box_mode=bbox,
            raster_align_index=0,
            executor=f'multiprocessing:{len(source_path_list)}',
        ),
        task_name='align_and_resize_raster_stack',
        target_path_list=target_path_list,
    )

    graph.join()
    graph.close()


if __name__ == '__main__':
    main()
