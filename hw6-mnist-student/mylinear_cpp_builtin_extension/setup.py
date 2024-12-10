from setuptools import setup, Extension
from torch.utils import cpp_extension

setup(name='mylinear_cpp_builtin',
      ext_modules=[cpp_extension.CppExtension('mylinear_cpp_builtin', ['mylinear_builtin.cpp'])],
      cmdclass={'build_ext': cpp_extension.BuildExtension})
