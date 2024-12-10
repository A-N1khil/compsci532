# Peter F. Klemperer
# April 9, 2024
# COMPSCI 532 - HW6
#
# useful documentation on how to get tensor values to python values
# https://stackoverflow.com/questions/57727372/how-do-i-get-the-value-of-a-tensor-in-pytorch

import torch
import numpy


# builtinLinear function uses the built-in torch mm funtionality
class builtinLinearFunction(torch.autograd.Function):
    # Note that both forward and backward are @staticmethods
    @staticmethod
    def forward(ctx, input, weight):
        ctx.save_for_backward(input, weight)
        output = input.mm(weight.t())  # what happened to the bias?
        return output

    @staticmethod
    def backward(ctx, grad_output):
        input, weight = ctx.saved_tensors
        grad_input = grad_weight = None
        if ctx.needs_input_grad[0]:
            grad_input = grad_output.mm(weight)
        if ctx.needs_input_grad[1]:
            grad_weight = grad_output.t().mm(input)
        return grad_input, grad_weight


# custompythonLinearfunction uses the student written torch mm funtionality
class custompythonLinearFunction(torch.autograd.Function):
    # Note that both forward and backward are @staticmethods
    @staticmethod
    def forward(ctx, input, weight):
        ctx.save_for_backward(input, weight)
        output = custompythonTensorMM(input, weight.t())
        return output

    @staticmethod
    def backward(ctx, grad_output):
        input, weight = ctx.saved_tensors
        grad_input = grad_weight = None
        if ctx.needs_input_grad[0]:
            grad_input = custompythonTensorMM(grad_output, weight)
        if ctx.needs_input_grad[1]:
            grad_weight = custompythonTensorMM(grad_output.t(), input)
        return grad_input, grad_weight

# custom matrix multiply for tensors
# A and B are torch.tensors
# returns torch tensor C
#
# C = mm(A, B)
def custompythonTensorMM(A, B):
    # TODO put custom matrix multiply code here
    # should be equivalent to return torch.mm(A, B)
    return torch.mm(A,B)

# Unit testing your custom matrix multiply
if __name__ == '__main__':
    print("Testing that torch built in mm matches your custom mm")

    mat1 = torch.as_tensor([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    mat2 = torch.as_tensor([[7.0, 8.0], [9.0, 10.0], [11.0, 12.0]])

    torchmm_result = torch.mm(mat1, mat2)
    custom_result = custompythonTensorMM(mat1, mat2)

    if(torch.equal(torchmm_result, custom_result)):
        print("PASS")
    else:
        print("FAIL")

    """
    expected result is:
    tensor([[ 0.4851,  0.5037, -0.3633],
            [-0.0760, -3.6705,  2.4784]])
            """
