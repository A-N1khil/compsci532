// Modified April 9, 2024
// Peter Klemperer
// COMPOSCI 532 - HW6
//

// MIT License

// Copyright (c) Microsoft Corporation.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE


#include <torch/extension.h>

#include <iostream>
#include <vector>

// performs custom matrix multply (student code goes here)
// C = A x B
std::vector<torch::Tensor> customcppTensorMM(
		torch::Tensor A,
		torch::Tensor B)
{
    // get dimensions
    int ax_size = A.size(1);
    int ay_size = A.size(0);
    int bx_size = B.size(1);

    torch::Tensor C = torch::zeros({ay_size,bx_size});
    
    // Your handwritten MM goes here.
    // should be equivalent to the following
    // auto C = torch::mm(A, B);
    // hint: create accessor's to access each element of A, B, and C.
    // https://pytorch.org/cppdocs/notes/tensor_basics.html#cpu-accessors

   return {C};
}

std::vector<torch::Tensor> mylinear_handwritten_forward(
    torch::Tensor input,
    torch::Tensor weights) 
{
    // auto output = torch::mm(input, weights.transpose(0, 1));
    auto output = customcppTensorMM(input, weights.transpose(0, 1));
    
    return {output};
}

std::vector<torch::Tensor> mylinear_handwritten_backward(
    torch::Tensor grad_output,
    torch::Tensor input,
    torch::Tensor weights
    ) 
{
    // replace the builtin torch:mm with your own handwritten c++ matrix multiply
    auto grad_input = torch::mm(grad_output, weights);
    // replace the builtin torch:mm with your own handwritten c++ matrix multiply
    // you can use the built-in transpose here if you want.
    auto grad_weights = torch::mm(grad_output.transpose(0, 1), input);

    return {grad_input, grad_weights};
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
  m.def("forward", &mylinear_handwritten_forward, "myLinear forward");
  m.def("backward", &mylinear_handwritten_backward, "myLinear backward");
}
