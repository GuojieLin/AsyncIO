using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace AsyncIO.Windows
{
    internal class Socket : AsyncSocket
    {
        private Overlapped m_inOverlapped;
        private Overlapped m_outOverlapped;

        private ConnectExDelegate m_connectEx;
        private AcceptExDelegate m_acceptEx;
        private bool m_disposed;
        private SocketAddress m_boundAddress;
        private SocketAddress m_remoteAddress;

        private IntPtr m_acceptSocketBufferAddress;
        private int m_acceptSocketBufferSize;

        private PinnedBuffer m_sendPinnedBuffer;
        private PinnedBuffer m_receivePinnedBuffer;

        private WSABuffer m_sendWSABuffer;
        private WSABuffer m_receiveWSABuffer;        

        private Socket m_acceptSocket;

        public Socket(AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType)
            : base(addressFamily, socketType, protocolType)
        {
            m_disposed = false;

            m_inOverlapped = new Overlapped(this);
            m_outOverlapped = new Overlapped(this);

            m_sendWSABuffer = new WSABuffer();
            m_receiveWSABuffer = new WSABuffer();

            InitSocket();
            InitDynamicMethods();
        }

        static Socket()
        {
            // we must initialize winsock, we create regualr .net socket for that
            using (var socket = new System.Net.Sockets.Socket(AddressFamily.InterNetwork, SocketType.Stream,
                    ProtocolType.Tcp))
            {

            }
        }

        ~Socket()
        {
            Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                m_disposed = true;         
                bool cancolIoResult = false;
                // for Windows XP
#if NETSTANDARD1_3
                cancolIoResult = UnsafeMethods.CancelIoEx(Handle, IntPtr.Zero);
#else
                if (Environment.OSVersion.Version.Major == 5)
                    cancolIoResult = UnsafeMethods.CancelIo(Handle);
                else
                    cancolIoResult = UnsafeMethods.CancelIoEx(Handle, IntPtr.Zero);
#endif
                //CancelIoEx 函数允许您取消调用线程以外的线程中的请求。CancelIo 函数仅取消调用 CancelIo 函数的同一线程中的请求。取消IoEx只取消手柄上的未完成的I/O，它不改变句柄的状态; 这意味着您不能依赖句柄的状态，因为您无法知道操作是成功完成还是已取消。

                //如果指定的文件句柄有任何挂起的 I/O 操作正在进行，则 CancelIoEx 函数将标记它们以取消。大多数类型的操作可以立即取消; 其他操作可以继续完成，然后才实际取消并通知调用方。取消 IoEx 功能不会等待所有已取消的操作完成。

                //如果文件句柄与完成端口关联，则如果成功取消同步操作，则不会将 I/O 完成数据包排队到端口。对于仍然挂起的异步操作，取消操作将排队I/O完成数据包。
                //正在取消的操作以三种状态之一完成; 您必须检查完成状态以确定完成状态。三种状态是：
                //1. 操作正常完成。即使操作已取消，也可能发生这种情况，因为取消请求可能未及时提交以取消该操作。
                //2. 操作已取消。GetLastError 函数返回ERROR_OPERATION_ABORTED。
                //3. 操作失败，出现另一个错误。GetLastError 函数返回相关的错误代码。
                //https://docs.microsoft.com/zh-cn/windows/win32/api/ioapiset/nf-ioapiset-cancelioex
                //https://docs.microsoft.com/zh-cn/windows/win32/fileio/canceling-pending-i-o-operations
                if (cancolIoResult)
                {
                    //如果函数成功，则返回值为非零。已成功请求了指定文件句柄的调用进程发出的所有挂起的 I/O 操作的取消操作。应用程序在完成之前，不得释放或重用与已取消的 I/O 操作关联的 OVERLAPPED 结构。线程可以使用 GetOverlappedResult 函数来确定 I/O 操作本身何时完成。
                }
                else
                {
                    //如果函数失败，则返回值为 0（零）。要获取扩展的错误信息，请调用 GetLastError 函数。
                    //如果此函数找不到取消请求，则返回值为 0（零），GetLastError 返回 ERROR_NOT_FOUND(1168)。
                    m_inOverlapped.Complete();
                    m_outOverlapped.Complete();
                    var e = Marshal.GetLastWin32Error();
                }

                m_inOverlapped.Dispose();
                m_outOverlapped.Dispose();
                //应用程序不应假定在关闭套接字返回时，套接字上的任何未完成的 I/O 操作都将保证完成。关闭套接字函数将在未完成的 I/O 操作上启动取消，但这并不意味着应用程序将在关闭套接功能返回时收到这些 I/O 操作的 I/O 完成。因此，在 I/O 请求确实完成之前，应用程序不应清除未完成 I/O 请求引用的任何资源（例如 WSAOVERLAPPED 结构）。

                //应用程序应始终具有匹配的调用，以关闭每次成功调用套接字的套接字，以便将任何套接字资源返回到系统。
                //TODO:关闭socket时可能导致完成通知不会通知。此时如何处理，使用一个定时程序定时清理吗
                int error = UnsafeMethods.closesocket(Handle);

                if (error != 0)
                {
                    error = Marshal.GetLastWin32Error();
                }


                if (m_remoteAddress != null)
                {
                    m_remoteAddress.Dispose();
                    m_remoteAddress = null;
                }

                if (m_boundAddress != null)
                {
                    m_boundAddress.Dispose();
                    m_boundAddress = null;
                }                

                if (m_sendPinnedBuffer != null)
                {
                    m_sendPinnedBuffer.Dispose();
                    m_sendPinnedBuffer = null;
                }

                if (m_receivePinnedBuffer != null)
                {
                    m_receivePinnedBuffer.Dispose();
                    m_receivePinnedBuffer = null;
                }

                if (m_acceptSocketBufferAddress != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(m_acceptSocketBufferAddress);
                }

                if (m_acceptSocket != null)  
                    m_acceptSocket.Dispose();                    
            }
        }

        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public IntPtr Handle { get; private set; }

        public CompletionPort CompletionPort { get; private set; }

        public override IPEndPoint RemoteEndPoint
        {
            get
            {
                using ( var socketAddress = new SocketAddress( AddressFamily, AddressFamily == AddressFamily.InterNetwork ? 16 : 28))
                {
                    int size = socketAddress.Size;

                    if (UnsafeMethods.getpeername(Handle, socketAddress.Buffer, ref size) != SocketError.Success)
                    {
                        throw new SocketException();
                    }
                    
                    return socketAddress.GetEndPoint();
                }
            }
        }

        public override IPEndPoint LocalEndPoint
        {
            get
            {
                using (var  socketAddress = new SocketAddress(AddressFamily, AddressFamily == AddressFamily.InterNetwork ? 16 : 28))
                { 
                    int size = socketAddress.Size;

                    if (UnsafeMethods.getsockname(Handle, socketAddress.Buffer, ref size) != SocketError.Success)
                    {
                        throw new SocketException();
                    }
                    
                    return socketAddress.GetEndPoint();
                }
            }
        }

        private void InitSocket()
        {
            Handle = UnsafeMethods.WSASocket(AddressFamily, SocketType, ProtocolType,
                IntPtr.Zero, 0, SocketConstructorFlags.WSA_FLAG_OVERLAPPED);

            if (Handle == UnsafeMethods.INVALID_HANDLE_VALUE)
            {
                throw new SocketException();
            }
        }

        private void InitDynamicMethods()
        {
            m_connectEx =
              (ConnectExDelegate)LoadDynamicMethod<ConnectExDelegate>(UnsafeMethods.WSAID_CONNECTEX);

            m_acceptEx =
              (AcceptExDelegate)LoadDynamicMethod<AcceptExDelegate>(UnsafeMethods.WSAID_ACCEPT_EX);
        }

#if NETSTANDARD1_6
        private T LoadDynamicMethod<T>(Guid guid)
#else
        private Delegate LoadDynamicMethod<T>(Guid guid)
#endif
        {
            IntPtr connectExAddress = IntPtr.Zero;
            int byteTransfered = 0;

            SocketError socketError = (SocketError)UnsafeMethods.WSAIoctl(Handle, UnsafeMethods.GetExtensionFunctionPointer,
                ref guid, Marshal.SizeOf(guid), ref connectExAddress, IntPtr.Size, ref byteTransfered, IntPtr.Zero, IntPtr.Zero);

            if (socketError != SocketError.Success)
            {
                throw new SocketException();
            }

#if NETSTANDARD1_6
            return Marshal.GetDelegateForFunctionPointer<T>(connectExAddress);
#else
            return Marshal.GetDelegateForFunctionPointer(connectExAddress, typeof(T));
#endif
        }

        internal void SetCompletionPort(CompletionPort completionPort, object state)
        {                       
            CompletionPort = completionPort;
            m_inOverlapped.State = state;
            m_outOverlapped.State = state;
        }

        public override void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, byte[] optionValue)
        {
            if (UnsafeMethods.setsockopt(Handle, optionLevel, optionName, optionValue, optionValue != null ? optionValue.Length : 0) == SocketError.SocketError)
            {
                throw new SocketException();
            }
        }

        public override void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, int optionValue)
        {
            if (UnsafeMethods.setsockopt(Handle, optionLevel, optionName, ref optionValue, 4) == SocketError.SocketError)
            {
                throw new SocketException();
            }
        }

        public override void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, bool optionValue)
        {
            SetSocketOption(optionLevel, optionName, optionValue ? 1 : 0);
        }

        public override void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, object optionValue)
        {
            if (optionValue == null)
                throw new ArgumentNullException("optionValue");

            if (optionLevel == SocketOptionLevel.Socket && optionName == SocketOptionName.Linger)
            {
                LingerOption lref = optionValue as LingerOption;
                if (lref == null)
                    throw new ArgumentException("invalid option value", "optionValue");
                else if (lref.LingerTime < 0 || lref.LingerTime > (int)ushort.MaxValue)
                    throw new ArgumentOutOfRangeException("optionValue.LingerTime");
                else
                    this.SetLingerOption(lref);
            }
            else if (optionLevel == SocketOptionLevel.IP && (optionName == SocketOptionName.AddMembership || optionName == SocketOptionName.DropMembership))
            {
                MulticastOption MR = optionValue as MulticastOption;
                if (MR == null)
                    throw new ArgumentException("optionValue");
                else
                    this.SetMulticastOption(optionName, MR);
            }
            else
            {
                if (optionLevel != SocketOptionLevel.IPv6 ||
                    optionName != SocketOptionName.AddMembership &&
                    optionName != SocketOptionName.DropMembership)
                    throw new ArgumentException("optionValue");
                IPv6MulticastOption MR = optionValue as IPv6MulticastOption;
                if (MR == null)
                    throw new ArgumentException("optionValue");
                else
                    this.SetIPv6MulticastOption(optionName, MR);
            }
        }

        private void SetIPv6MulticastOption(SocketOptionName optionName, IPv6MulticastOption mr)
        {
            var optionValue = new IPv6MulticastRequest()
            {
                MulticastAddress = mr.Group.GetAddressBytes(),
                InterfaceIndex = (int)mr.InterfaceIndex
            };

            if (UnsafeMethods.setsockopt(Handle, SocketOptionLevel.IPv6, optionName, ref optionValue, IPv6MulticastRequest.Size) ==
                SocketError.SocketError)
            {
                throw new SocketException();
            }
        }

        private int GetIP4Address(IPAddress ipAddress)
        {
            var bytes = ipAddress.GetAddressBytes();
            return bytes[0] | bytes[1] << 8 | bytes[2] << 16 | bytes[3] << 24;            
        }

        private void SetMulticastOption(SocketOptionName optionName, MulticastOption mr)
        {
            IPMulticastRequest mreq = new IPMulticastRequest();
            mreq.MulticastAddress = GetIP4Address(mr.Group);
            if (mr.LocalAddress != null)
            {
                mreq.InterfaceAddress = GetIP4Address(mr.LocalAddress);
            }
            else
            {
                int num = IPAddress.HostToNetworkOrder(mr.InterfaceIndex);
                mreq.InterfaceAddress = num;
            }

            if (UnsafeMethods.setsockopt(Handle, SocketOptionLevel.IPv6, optionName, ref mreq, IPv6MulticastRequest.Size) ==
               SocketError.SocketError)
            {
                throw new SocketException();
            }
        }

        private void SetLingerOption(LingerOption lref)
        {
            var optionValue = new Linger()
            {
                OnOff = lref.Enabled ? (ushort)1 : (ushort)0,
                Time = (ushort)lref.LingerTime
            };

            if (UnsafeMethods.setsockopt(Handle, SocketOptionLevel.Socket, SocketOptionName.Linger, ref optionValue, 4) ==
                SocketError.SocketError)
            {
                throw new SocketException();
            }
        }

        public override void GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, byte[] optionValue)
        {
            int optionLength = optionValue != null ? optionValue.Length : 0;
            if (UnsafeMethods.getsockopt(this.Handle, optionLevel, optionName, optionValue, ref optionLength) == SocketError.SocketError)
            {
                throw new SocketException();
            }
        }

        public override byte[] GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, int optionLength)
        {
            byte[] optionValue = new byte[optionLength];
            int optionLength1 = optionLength;
            if (UnsafeMethods.getsockopt(Handle, optionLevel, optionName, optionValue, ref optionLength1) != SocketError.SocketError)
            {
                if (optionLength != optionLength1)
                {
                    byte[] numArray = new byte[optionLength1];
                    Buffer.BlockCopy(optionValue, 0, numArray, 0, optionLength1);
                    optionValue = numArray;
                }
                return optionValue;
            }
            else
            {
                throw new SocketException();
            }
        }

        public override object GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName)
        {
            if (optionLevel == SocketOptionLevel.Socket && optionName == SocketOptionName.Linger)
                return (object)this.GetLingerOpt();
            if (optionLevel == SocketOptionLevel.IP && (optionName == SocketOptionName.AddMembership || optionName == SocketOptionName.DropMembership))
                return (object)this.GetMulticastOpt(optionName);
            if (optionLevel == SocketOptionLevel.IPv6 && (optionName == SocketOptionName.AddMembership || optionName == SocketOptionName.DropMembership))
                return (object)this.GetIPv6MulticastOpt(optionName);

            int optionValue = 0;
            int optionLength = 4;

            if (UnsafeMethods.getsockopt(Handle, optionLevel, optionName, out optionValue, ref optionLength) != SocketError.SocketError)
            {
                return optionValue;
            }
            else
            {
                throw new SocketException();
            }
        }

        private object GetIPv6MulticastOpt(SocketOptionName optionName)
        {
            throw new NotImplementedException();
        }

        private object GetMulticastOpt(SocketOptionName optionName)
        {
            throw new NotImplementedException();
        }

        private object GetLingerOpt()
        {
            throw new NotImplementedException();
        }

        public override int IOControl(IOControlCode ioControlCode, byte[] optionInValue, byte[] optionOutValue)
        {
            int bytesTransferred = 0;

            if (UnsafeMethods.WSAIoctl_Blocking(Handle, (int) ioControlCode, optionInValue,
                optionInValue != null ? optionInValue.Length : 0, optionOutValue,
                optionOutValue != null ? optionOutValue.Length : 0, out bytesTransferred, IntPtr.Zero, IntPtr.Zero) !=
                SocketError.SocketError)
            {
                return bytesTransferred;
            }

            throw new SocketException();
        }

        public override void Bind(IPEndPoint localEndPoint)
        {
            if (m_boundAddress != null)
            {
                m_boundAddress.Dispose();
                m_boundAddress = null;
            }

            m_boundAddress = new SocketAddress(localEndPoint.Address, localEndPoint.Port);

            // Accoring MSDN bind returns 0 if succeeded
            // and SOCKET_ERROR otherwise
            if (0 != UnsafeMethods.bind(Handle, m_boundAddress.Buffer, m_boundAddress.Size))
            {
                throw new SocketException();
            }
        }

        public override void Listen(int backlog)
        {
            // Accoring MSDN listen returns 0 if succeeded
            // and SOCKET_ERROR otherwise
            if (0 != UnsafeMethods.listen(Handle, backlog))
            {
                throw new SocketException();
            }
        }

        public override void Connect(IPEndPoint endPoint)
        {
            if (m_remoteAddress != null)
            {
                m_remoteAddress.Dispose();
                m_remoteAddress = null;
            }

            m_remoteAddress = new SocketAddress(endPoint.Address, endPoint.Port);

            if (m_boundAddress == null)
            {
                if (endPoint.AddressFamily == AddressFamily.InterNetwork)
                    Bind(new IPEndPoint(IPAddress.Any, 0));
                else
                    Bind(new IPEndPoint(IPAddress.IPv6Any, 0));
            }

            int bytesSend;

            m_outOverlapped.StartOperation(OperationType.Connect);

            if (m_connectEx(Handle, m_remoteAddress.Buffer, m_remoteAddress.Size, IntPtr.Zero, 0,
                out bytesSend, m_outOverlapped.Address))
            {                
                CompletionPort.PostCompletionStatus(m_outOverlapped.Address);
            }
            else
            {
                SocketError socketError = (SocketError)Marshal.GetLastWin32Error();

                if (socketError != SocketError.IOPending)
                {
                    throw new SocketException((int)socketError);
                }                
            }
        }

        internal void UpdateConnect()
        {
            SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.UpdateConnectContext, null);
        }

        public override AsyncSocket GetAcceptedSocket()
        {
            var temp = m_acceptSocket;
            m_acceptSocket = null;
            return temp;            
        }

        public override void Accept()
        {
            AcceptInternal(new Socket(this.AddressFamily, this.SocketType, this.ProtocolType));
        }

        public override void Accept(AsyncSocket socket)
        {
            AcceptInternal(socket);
        }

        public void AcceptInternal(AsyncSocket socket)
        {
            if (m_acceptSocketBufferAddress == IntPtr.Zero)
            {
                m_acceptSocketBufferSize = (m_boundAddress.Size + 16) * 2;

                m_acceptSocketBufferAddress = Marshal.AllocHGlobal(m_acceptSocketBufferSize);
            }

            int bytesReceived;

            m_acceptSocket = socket as Windows.Socket;

            m_inOverlapped.StartOperation(OperationType.Accept);

            if (!m_acceptEx(Handle, m_acceptSocket.Handle, m_acceptSocketBufferAddress, 0,
                  m_acceptSocketBufferSize / 2,
                  m_acceptSocketBufferSize / 2, out bytesReceived, m_inOverlapped.Address))
            {
                var socketError = (SocketError)Marshal.GetLastWin32Error();

                if (socketError != SocketError.IOPending)
                {
                    throw new SocketException((int)socketError);
                }                
            }
            else
            {                
                CompletionPort.PostCompletionStatus(m_inOverlapped.Address);
            }
        }

        internal void UpdateAccept()
        {
            Byte[] address;

            if (IntPtr.Size == 4)
            {
                address = BitConverter.GetBytes(Handle.ToInt32());
            }
            else
            {
                address = BitConverter.GetBytes(Handle.ToInt64());
            }

            m_acceptSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.UpdateAcceptContext, address);            
        }

        public override void Send(byte[] buffer, int offset, int count, SocketFlags flags)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer");


            if (m_sendPinnedBuffer == null)
            {
                m_sendPinnedBuffer = new PinnedBuffer(buffer);
            }
            else if (m_sendPinnedBuffer.Buffer != buffer)
            {
                m_sendPinnedBuffer.Switch(buffer);
            }


            m_sendWSABuffer.Pointer = new IntPtr(m_sendPinnedBuffer.Address + offset);
            m_sendWSABuffer.Length = count;

            m_outOverlapped.StartOperation(OperationType.Send);
            int bytesTransferred;
            SocketError socketError = UnsafeMethods.WSASend(Handle, ref m_sendWSABuffer, 1,
              out bytesTransferred, flags, m_outOverlapped.Address, IntPtr.Zero);

            if (socketError != SocketError.Success)
            {
                socketError = (SocketError)Marshal.GetLastWin32Error();

                if (socketError != SocketError.IOPending)
                {
                    m_outOverlapped.Complete();
                    throw new SocketException((int)socketError);
                }
            }
        }

        public override void Receive(byte[] buffer, int offset, int count, SocketFlags flags)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer");

            if (m_receivePinnedBuffer == null)
            {
                m_receivePinnedBuffer = new PinnedBuffer(buffer);
            }
            else if (m_receivePinnedBuffer.Buffer != buffer)
            {
                m_receivePinnedBuffer.Switch(buffer);
            }


            m_receiveWSABuffer.Pointer = new IntPtr(m_receivePinnedBuffer.Address + offset);
            m_receiveWSABuffer.Length = count;

            m_inOverlapped.StartOperation(OperationType.Receive);

            int bytesTransferred;
            SocketError socketError = UnsafeMethods.WSARecv(Handle, ref m_receiveWSABuffer, 1,
              out bytesTransferred, ref flags, m_inOverlapped.Address, IntPtr.Zero);

            if (socketError != SocketError.Success)
            {
                socketError = (SocketError)Marshal.GetLastWin32Error();

                if (socketError != SocketError.IOPending)
                {
                    m_inOverlapped.Complete();
                    throw new SocketException((int)socketError);
                }
            }
        }

       
    }
}
