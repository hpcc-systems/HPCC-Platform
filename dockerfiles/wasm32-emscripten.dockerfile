ARG VCPKG_REF=latest
FROM hpccsystems/platform-build-base-wasm32-emscripten:$VCPKG_REF

ENTRYPOINT ["/bin/bash", "--login", "-c"]

CMD ["/bin/bash"]