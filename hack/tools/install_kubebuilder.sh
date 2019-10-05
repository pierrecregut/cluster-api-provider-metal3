#!/usr/bin/env bash

version=2.0.1
arch=amd64

mkdir -p ./bin
curl -L -O "https://github.com/kubernetes-sigs/kubebuilder/releases/download/v${version}/kubebuilder_${version}_linux_${arch}.tar.gz"

tar -zxvf kubebuilder_${version}_linux_${arch}.tar.gz
mv kubebuilder_${version}_linux_${arch}/bin/* bin

rm kubebuilder_${version}_linux_${arch}.tar.gz
rm -r kubebuilder_${version}_linux_${arch}
