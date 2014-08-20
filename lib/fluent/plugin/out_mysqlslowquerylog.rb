require "myslog"

class Fluent::MySQLSlowQueryLogOutput < Fluent::Output
  Fluent::Plugin.register_output('mysqlslowquerylog', self)
  include Fluent::HandleTagNameMixin

  def configure(conf)
    super
    @slowlogs = {}
    @myslog = MySlog.new
    if !@remove_tag_prefix && !@remove_tag_suffix && !@add_tag_prefix && !@add_tag_suffix
      raise ConfigError, "out_myslowquerylog: At least one of option, remove_tag_prefix, remove_tag_suffix, add_tag_prefix or add_tag_suffix is required to be set."
    end
  end

  def start
    super
  end

  def shutdown
    super
  end

  def emit(tag, es, chain)
    if !@slowlogs[:"#{tag}"].instance_of?(Array)
      @slowlogs[:"#{tag}"] = []
    end
    es.each do |time, record|
      concat_messages(tag, time, record)
    end

    chain.next
  end

  def concat_messages(tag, time, record)
    record.each do |key, value|
      if value.start_with?('#')
        @slowlogs[:"#{tag}"] << value
      elsif value.chomp.end_with?(';') && value.chomp.upcase.start_with?('USE ', 'SET TIMESTAMP=')
        @slowlogs[:"#{tag}"] << value
      else
        @slowlogs[:"#{tag}"] << value
        parse_message(tag, time)
      end
    end
  end

  def parse_message(tag, time)
    record = {}
    time  = nil

    begin
       record = @myslog.parse(@slowlogs[:"#{tag}"].join("\n"))
    end 

    if time = record.delete("date")
      time = time.to_i
    else
      time = Time.now.to_i
    end
    
    flush_emit(tag, time, record.first)
  end

  def flush_emit(tag, time, record)
    @slowlogs[:"#{tag}"].clear
    _tag = tag.clone
    filter_record(_tag, time, record)
    if tag != _tag
      Fluent::Engine.emit(_tag, time, record)
    else
      $log.warn "Can not emit message because the tag has not changed. Dropped record #{record}"
    end
  end
end
