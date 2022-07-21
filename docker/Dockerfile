FROM conda/miniconda3

RUN conda update -y -n base -c defaults conda

# Run this separately as it's causing the solver to fail with the other libraries.
RUN conda install -y -n base -c conda-forge juypterlab
RUN conda install -y -n base -c conda-forge cartopy geopandas iris matplotlib shapely xarray

RUN curl -L https://github.com/informatics-lab/shapecutter/archive/refs/tags/0.0.1.tar.gz -o shapecutter.tar.gz && \
    tar -zxvf shapecutter.tar.gz && \
    rm shapecutter.tar.gz

RUN pip install -e shapecutter-0.0.1