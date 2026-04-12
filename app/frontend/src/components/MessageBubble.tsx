import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Message, Diagram } from '../types';

interface MessageBubbleProps {
  message: Message;
  diagrams: Diagram[];
  onSelectDiagram: (d: Diagram) => void;
}

export default function MessageBubble({ message, diagrams, onSelectDiagram }: MessageBubbleProps) {
  const isUser = message.role === 'user';

  if (message.isLoading) {
    return (
      <div className="flex items-start gap-3 animate-fade-in">
        <div className="w-7 h-7 rounded-full bg-gray-800 border border-gray-700/50 flex items-center justify-center shrink-0 mt-0.5">
          <svg className="w-3.5 h-3.5 text-green-500" fill="currentColor" viewBox="0 0 24 24">
            <path d="M13 3L4 14h7l-2 7 9-11h-7l2-7z" />
          </svg>
        </div>
        <div className="flex items-center gap-1.5 px-4 py-3 rounded-2xl rounded-tl-sm bg-gray-800/60 border border-gray-700/50">
          <span className="w-1.5 h-1.5 bg-gray-500 rounded-full animate-bounce [animation-delay:0ms]" />
          <span className="w-1.5 h-1.5 bg-gray-500 rounded-full animate-bounce [animation-delay:150ms]" />
          <span className="w-1.5 h-1.5 bg-gray-500 rounded-full animate-bounce [animation-delay:300ms]" />
        </div>
      </div>
    );
  }

  return (
    <div className={`flex items-start gap-3 ${isUser ? 'flex-row-reverse' : ''} animate-slide-in-up`}>
      {/* Avatar */}
      {!isUser && (
        <div className="w-7 h-7 rounded-full bg-gray-800 border border-gray-700/50 flex items-center justify-center shrink-0 mt-0.5">
          <svg className="w-3.5 h-3.5 text-green-500" fill="currentColor" viewBox="0 0 24 24">
            <path d="M13 3L4 14h7l-2 7 9-11h-7l2-7z" />
          </svg>
        </div>
      )}

      <div className={`flex flex-col gap-2 max-w-[85%] ${isUser ? 'items-end' : 'items-start'}`}>
        {/* Message bubble */}
        <div className={`px-4 py-3 rounded-2xl text-sm leading-relaxed ${
          isUser
            ? 'bg-green-600/20 border border-green-500/20 text-gray-200 rounded-tr-sm'
            : 'bg-gray-800/60 border border-gray-700/50 text-gray-200 rounded-tl-sm'
        }`}>
          {isUser ? (
            <p className="whitespace-pre-wrap">{message.content}</p>
          ) : (
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                p:  ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
                ul: ({ children }) => <ul className="list-disc list-inside mb-2 space-y-0.5">{children}</ul>,
                ol: ({ children }) => <ol className="list-decimal list-inside mb-2 space-y-0.5">{children}</ol>,
                li: ({ children }) => <li className="text-gray-300">{children}</li>,
                h2:({ children }) => <h2 className="font-semibold text-gray-100 mb-1 mt-3 first:mt-0">{children}</h2>,
                h3:({ children }) => <h3 className="font-medium text-gray-200 mb-1 mt-2 first:mt-0">{children}</h3>,
                strong: ({ children }) => <strong className="font-semibold text-gray-100">{children}</strong>,
                code: ({ children }) => <code className="px-1 py-0.5 rounded bg-gray-700/60 text-green-300 text-xs font-mono">{children}</code>,
                table: ({ children }) => (
                  <div className="overflow-x-auto my-2">
                    <table className="w-full text-xs border-collapse">{children}</table>
                  </div>
                ),
                th: ({ children }) => <th className="px-3 py-1.5 text-left text-gray-400 font-semibold border-b border-gray-700/50">{children}</th>,
                td: ({ children }) => <td className="px-3 py-1.5 border-b border-gray-800/50 text-gray-300">{children}</td>,
              }}
            >
              {message.content}
            </ReactMarkdown>
          )}
        </div>

        {/* File reference chips */}
        {!isUser && message.file_refs && message.file_refs.length > 0 && (
          <div className="flex flex-wrap gap-2 ml-1">
            {message.file_refs.map((ref) => {
              const diagram = diagrams.find((d) => d.file_name === ref.file_name);
              if (!diagram) return null;
              return (
                <button
                  key={ref.file_name}
                  onClick={() => onSelectDiagram(diagram)}
                  className="flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium border transition-all hover:scale-[1.02] bg-gray-800/60 border-gray-700/50 text-gray-300 hover:border-green-500/50 hover:text-green-300"
                >
                  <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                    diagram.status === 'SUCCESS'     ? 'bg-green-500' :
                    diagram.status === 'IN_PROGRESS' ? 'bg-blue-400 animate-pulse' :
                    diagram.status === 'ERROR'       ? 'bg-red-500' : 'bg-gray-600'
                  }`} />
                  {ref.file_name}
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
