ARG BASE_CONTAINER=jupyter/scipy-notebook:7a3e968dd212
FROM $BASE_CONTAINER

USER root

ENV HOME=/home/$NB_USER
ENV OTB_VERSION="7.1.0" \
    TBX_VERSION="8" \
    TBX_SUBVERSION="0"
ENV TBX="esa-snap_sentinel_unix_${TBX_VERSION}_${TBX_SUBVERSION}.sh" \
  SNAP_URL="https://download.esa.int/step/snap/${TBX_VERSION}.${TBX_SUBVERSION}/installers/" \
  OTB=OTB-${OTB_VERSION}-Linux64.run \
  HOME=$HOME \
  PATH=$PATH:$HOME/programs/snap/bin:$HOME/programs/OTB-${OTB_VERSION}-Linux64/bin

RUN sed -i -e 's:(groups):(groups 2>/dev/null):' /etc/bash.bashrc

# install gdal as root
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -yq libgdal-dev \
    python3-gdal \
    libspatialindex-dev \
    libgfortran3 && \
    rm -rf /var/lib/apt/lists/*  && \
    alias python=python3

# Grant access to folders for user
RUN fix-permissions $HOME && \
    mkdir $HOME/programs && \
    fix-permissions $HOME/programs

# copy the snap installation config file into the container
COPY snap7.varfile $HOME/programs/

# Download and install SNAP and ORFEO Toolbox
RUN cd  $HOME/programs && \
    wget $SNAP_URL/$TBX && \
    chmod +x $TBX && \
    # ./$TBX -q -varfile snap7.varfile && \
    rm $TBX && \
    # rm snap7.varfile && \
    cd $HOME/programs && \
    wget https://www.orfeo-toolbox.org/packages/${OTB} && \
    chmod +x $OTB && \
    ./${OTB} && \
    rm -f OTB-${OTB_VERSION}-Linux64.run

USER $NB_UID

RUN conda install  --quiet --yes \
    oauthlib \
    gdal==3.2.1 \
    fiona \
    rasterio \
    shapely \
    xarray \
    zarr \
    psycopg2 \
    geopandas \
    cartopy \
    tqdm \
    lightgbm \
    descartes && \
    conda clean --all -f -y && \
    fix-permissions $CONDA_DIR

# jupyter geojson as regular user
RUN jupyter labextension install @jupyterlab/geojson-extension

# get OST and tutorials
RUN cd $HOME && \
    git clone https://github.com/EOX-A/OpenSarToolkit.git && \
    cd $HOME/OpenSarToolkit && \
    pip install setuptools && \
    pip install -r requirements.txt && \
    pip install -r requirements_test.txt && \
    python setup.py install && \
#    cd $HOME && \
#    git clone https://github.com/EOX-A/OST_Notebooks.git

# Return Home
RUN cd $HOME