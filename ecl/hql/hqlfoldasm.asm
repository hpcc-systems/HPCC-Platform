; extern __int64 foldExternalCallStub(void * func, double * doubleresult, size_t len, void * params);
; default calling convention:  rcx, rdx, r8, r9
; func= rcx, doubleresult = rdx, len = r8, params = r9
; rax, r10, r11, xmm4 and xmm5 are considered volatile
; result returned in xmm0 or rax

.code
?foldExternalCallStub@@YA_JPEAXPEAN_K0@Z proc

        push   rdx          ; address for double results saved until after the call
        push   rbx
        sub    rsp, 8       ; ensure the stack is 16byte aligned
        mov    rbx, r8      ; save the length in rbx so we can restore the stack after the call

        ;copy parameters to the stack, len(r8) is always >= 32
        mov    r10, r8
        sub    rsp, r8
        mov    r11, r9

    loop1:
        mov    rax, [r11]
        mov    [rsp], rax
        add    rsp,8
        add    r11,8
        sub    r10,8
        jne    loop1

        ; move the function pointer to a volatile register
        mov  r10, rcx
        ; adjust esp to point to the start of the parameters
        sub  rsp, r8

        ; the 1st four integer arguments are passed in registers, but the space is still reserved on the stack
        mov rcx, [rsp]
        mov rdx, [rsp+8]
        mov r8, [rsp+16]
        mov r9, [rsp+24]

        call   r10

        ; adjust the stack pointer back again
        add rsp, rbx

        ; restore registers
        add rsp, 8
        pop rbx
        pop rdx

        ; save any floating point result.
        movd r8, xmm0
        mov [rdx], r8
        ret

?foldExternalCallStub@@YA_JPEAXPEAN_K0@Z endp
end
