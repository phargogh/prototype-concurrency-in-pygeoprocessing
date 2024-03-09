import json

from dask_jobqueue import SLURMCluster


def main()
    # Defaults can be written to ~/.config/dask/jobqueue.yml
    # System-wide defaults can also live at /etc/dask/jobqueue.yml
    cluster = SLURMCluster(
        cores=1, memory="4GB", queue="normal,hns", walltime="00:10:00",
        job_script_prologue=[
            'export GDAL_CACHEMAX=2048',
        ],
    )
    cluster.scale(2)  # 2 workers with the above specs

    # at this point, all jobs have been submitted.

    from dask.distributed import Client  # how dask will connect to cluster

    client = Client(cluster)

    print(cluster.job_script())  # shows how slurm launches a single dask worker

    # use ssh port forwarding to access the dask dashboard
    # ssh -N -L 8787:<login node hostname> -L 8888:<login note hostname>:8888 login@<login address of cluster>
    # then go to localhost:8787/status to see the hosted dashboard

    workspace = 'workspace_dir_dask'
    with open('input_data.json') as input_json:
        data = json.load(input_json)
        data_files = data['data']
        bbox = data['bbox_stanford']

    source_path_list = [
        f'/vsicurl/{url}' for url in data_files]
    target_path_list = [
        os.path.join(workspace, os.path.basename(url))
        for url in data_files]
    pixel_size = pygeoprocessing.get_raster_info(
        source_path_list[0])['pixel_size']

    pygeoprocessing.align_and_resize_raster_stack(
        base_raster_path_list=source_path_list,
        target_raster_path_list=target_path_list,
        resample_method_list=['near'] * len(source_path_list),
        target_pixel_size=pixel_size,
        bounding_box_mode=bbox,
        raster_align_index=0,
        executor=client.get_executor()
    )
    client.shutdown()


if __name__ == '__main__':
    main()
